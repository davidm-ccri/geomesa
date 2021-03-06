package geomesa.core.index

import java.nio.charset.StandardCharsets
import java.util.Map.Entry

import com.google.common.collect.Iterators
import com.vividsolutions.jts.geom.{Point, Polygon}
import geomesa.core._
import geomesa.core.data._
import geomesa.core.filter.{ff, _}
import geomesa.core.index.QueryHints._
import geomesa.core.iterators.{FEATURE_ENCODING, _}
import geomesa.core.util.{CloseableIterator, SelfClosingBatchScanner}
import geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial.DWithin

import scala.collection.JavaConversions._
import scala.util.Random


object IndexQueryPlanner {
  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
}

import geomesa.core.index.IndexQueryPlanner._
import geomesa.utils.geotools.Conversions._

case class IndexQueryPlanner(keyPlanner: KeyPlanner,
                             cfPlanner: ColumnFamilyPlanner,
                             schema: String,
                             featureType: SimpleFeatureType,
                             featureEncoder: SimpleFeatureEncoder) extends ExplainingLogging {

  def buildFilter(poly: Polygon, interval: Interval): KeyPlanningFilter =
    (IndexSchema.somewhere(poly), IndexSchema.somewhen(interval)) match {
      case (None, None)       =>    AcceptEverythingFilter
      case (None, Some(i))    =>
        if (i.getStart == i.getEnd) DateFilter(i.getStart)
        else                        DateRangeFilter(i.getStart, i.getEnd)
      case (Some(p), None)    =>    SpatialFilter(poly)
      case (Some(p), Some(i)) =>
        if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
        else                        SpatialDateRangeFilter(p, i.getStart, i.getEnd)
    }


  def netPolygon(poly: Polygon): Polygon = poly match {
    case null => null
    case p if p.covers(IndexSchema.everywhere) =>
      IndexSchema.everywhere
    case p if IndexSchema.everywhere.covers(p) => p
    case _ => poly.intersection(IndexSchema.everywhere).
      asInstanceOf[Polygon]
  }

  def netInterval(interval: Interval): Interval = interval match {
    case null => null
    case _    => IndexSchema.everywhen.overlap(interval)
  }

  // As a pre-processing step, we examine the query/filter and split it into multiple queries.
  // TODO: Work to make the queries non-overlapping.
  def getIterator(acc: AccumuloConnectorCreator,
                  sft: SimpleFeatureType,
                  query: Query,
                  output: ExplainerOutputType = log): CloseableIterator[Entry[Key,Value]] = {
    val ff = CommonFactoryFinder.getFilterFactory2
    val isDensity = query.getHints.containsKey(BBOX_KEY)
    val queries: Iterator[Query] =
      if(isDensity) {
        val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
        val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
        Iterator(DataUtilities.mixQueries(q1, query, "geomesa.mixed.query"))
      } else {
        splitQueryOnOrs(query, output)
      }

    queries.flatMap(runQuery(acc, sft, _, isDensity, output))
  }
  
  def splitQueryOnOrs(query: Query, output: ExplainerOutputType): Iterator[Query] = {
    val originalFilter = query.getFilter
    output(s"Originalfilter is $originalFilter")

    val rewrittenFilter = rewriteFilter(originalFilter)
    output(s"Filter is rewritten as $rewrittenFilter")

    val orSplitter = new OrSplittingFilter
    val splitFilters = orSplitter.visit(rewrittenFilter, null)

    // Let's just check quickly to see if we can eliminate any duplicates.
    val filters = splitFilters.distinct

    filters.map { filter =>
      val q = new Query(query)
      q.setFilter(filter)
      q
    }.toIterator
  }

  /**
   * Helper method to execute a query against an AccumuloDataStore
   *
   * If the query contains ONLY an eligible LIKE
   * or EQUALTO query then satisfy the query with the attribute index
   * table...else use the spatio-temporal index table
   *
   * If the query is a density query use the spatio-temporal index table only
   */
  private def runQuery(acc: AccumuloConnectorCreator,
                       sft: SimpleFeatureType,
                       derivedQuery: Query,
                       isDensity: Boolean,
                       output: ExplainerOutputType) = {
    val filterVisitor = new FilterToAccumulo(featureType)
    val rewrittenFilter = filterVisitor.visit(derivedQuery)
    if(acc.catalogTableFormat(sft)){
      // If we have attr index table try it
      runAttrIdxQuery(acc, derivedQuery, rewrittenFilter, filterVisitor, isDensity, output)
    } else {
      // datastore doesn't support attr index use spatiotemporal only
      stIdxQuery(acc, derivedQuery, filterVisitor, output)
    }
  }

  /**
   * Attempt to run a query against the attribute index if it can be satisfied 
   * there...if not run against the SpatioTemporal
   */
  def runAttrIdxQuery(acc: AccumuloConnectorCreator,
                      derivedQuery: Query,
                      rewrittenFilter: Filter,
                      filterVisitor: FilterToAccumulo,
                      isDensity: Boolean,
                      output: ExplainerOutputType) = {

    rewrittenFilter match {
      case isEqualTo: PropertyIsEqualTo if !isDensity && attrIdxQueryEligible(isEqualTo) =>
        attrIdxEqualToQuery(acc, derivedQuery, isEqualTo, filterVisitor, output)

      case like: PropertyIsLike if !isDensity =>
        if(attrIdxQueryEligible(like) && likeEligible(like))
          attrIdxLikeQuery(acc, derivedQuery, like, filterVisitor, output)
        else
          stIdxQuery(acc, derivedQuery, filterVisitor, output)

      case cql =>
        stIdxQuery(acc, derivedQuery, filterVisitor, output)
    }
  }

  def attrIdxQueryEligible(filt: Filter) = filt match {
    case filter: PropertyIsEqualTo =>
      val one = filter.getExpression1
      val two = filter.getExpression2
      val prop = (one, two) match {
        case (p: PropertyName, _) => p.getPropertyName
        case (_, p: PropertyName) => p.getPropertyName
      }
      featureType.getDescriptor(prop).isIndexed

    case filter: PropertyIsLike =>
      val prop = filter.getExpression.asInstanceOf[PropertyName].getPropertyName
      featureType.getDescriptor(prop).isIndexed
  }


      // TODO try to use wildcard values from the Filter itself
  // Currently pulling the wildcard values from the filter
  // leads to inconsistent results...so use % as wildcard
  val MULTICHAR_WILDCARD = "%"
  val SINGLE_CHAR_WILDCARD = "_"
  val NULLBYTE = Array[Byte](0.toByte)

  /* Like queries that can be handled by current reverse index */
  def likeEligible(filter: PropertyIsLike) = containsNoSingles(filter) && trailingOnlyWildcard(filter)

  /* contains no single character wildcards */
  def containsNoSingles(filter: PropertyIsLike) =
    !(filter.getLiteral.replace("\\\\", "").replace(s"\\$SINGLE_CHAR_WILDCARD", "").contains(SINGLE_CHAR_WILDCARD))

  def trailingOnlyWildcard(filter: PropertyIsLike) =
    (filter.getLiteral.endsWith(MULTICHAR_WILDCARD) &&
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == filter.getLiteral.length - MULTICHAR_WILDCARD.length) ||
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == -1

  /**
   * Get an iterator that performs an eligible LIKE query against the Attribute Index Table
   */
  def attrIdxLikeQuery(acc: AccumuloConnectorCreator,
                       derivedQuery: Query,
                       filter: PropertyIsLike,
                       filterVisitor: FilterToAccumulo,
                       output: ExplainerOutputType) = {

    val expr = filter.getExpression
    val prop = expr match {
      case p: PropertyName => p.getPropertyName
    }

    // Remove the trailing wilcard and create a range prefix
    val literal = filter.getLiteral
    val value =
      if(literal.endsWith(MULTICHAR_WILDCARD))
        literal.substring(0, literal.length - MULTICHAR_WILDCARD.length)
      else
        literal

    val range = AccRange.prefix(formatAttrIdxRow(prop, value))

    attrIdxQuery(acc, derivedQuery, filterVisitor, range, output)
  }

  def formatAttrIdxRow(prop: String, lit: String) =
    new Text(prop.getBytes(StandardCharsets.UTF_8) ++ NULLBYTE ++ lit.getBytes(StandardCharsets.UTF_8))

  /**
   * Get an iterator that performs an EqualTo query against the Attribute Index Table
   */
  def attrIdxEqualToQuery(acc: AccumuloConnectorCreator,
                          derivedQuery: Query,
                          filter: PropertyIsEqualTo,
                          filterVisitor: FilterToAccumulo,
                          output: ExplainerOutputType) = {

    val one = filter.getExpression1
    val two = filter.getExpression2
    val (prop, lit) = (one, two) match {
      case (p: PropertyName, l: Literal) => (p.getPropertyName, l.getValue.toString)
      case (l: Literal, p: PropertyName) => (p.getPropertyName, l.getValue.toString)
      case _ =>
        val msg =
          s"""Unhandled equalTo Query (expr1 type: ${one.getClass.getName}, expr2 type: ${two.getClass.getName}
            |Supported types are literal = propertyName and propertyName = literal
          """.stripMargin
        throw new RuntimeException(msg)
    }

    val range = new AccRange(formatAttrIdxRow(prop, lit))

    attrIdxQuery(acc, derivedQuery, filterVisitor, range, output)
  }

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  def attrIdxQuery(acc: AccumuloConnectorCreator,
                   derivedQuery: Query,
                   filterVisitor: FilterToAccumulo,
                   range: AccRange,
                   output: ExplainerOutputType) = {
    logger.trace(s"Scanning attribute table for feature type ${featureType.getTypeName}")
    val attrScanner = acc.createAttrIdxScanner(featureType)

    val (geomFilters, otherFilters) = partitionGeom(derivedQuery.getFilter)
    val (temporalFilters, nonSTFilters) = partitionTemporal(otherFilters)

    // NB: Added check to see if the nonSTFilters is empty.
    //  If it is, we needn't configure the SFFI

    output(s"The geom filters are $geomFilters.\nThe temporal filters are $temporalFilters.")
    val ofilter: Option[Filter] = filterListAsAnd(geomFilters ++ temporalFilters)

    configureAttributeIndexIterator(attrScanner, ofilter, range)

    import scala.collection.JavaConversions._
    val ranges = attrScanner.iterator.map(_.getKey.getColumnFamily).map(new AccRange(_))

    val recScanner = if(ranges.hasNext) {
      val recordScanner = acc.createRecordScanner(featureType)
      recordScanner.setRanges(ranges.toList)
      configureSimpleFeatureFilteringIterator(recordScanner, featureType, None, derivedQuery)
      Some(recordScanner)
    } else None

    val iter = recScanner.map(_.iterator()).getOrElse(Iterators.emptyIterator[Entry[Key, Value]])

    def close(): Unit = {
      recScanner.foreach(_.close)
      attrScanner.close
    }

    CloseableIterator(iter, close)
  }

  def configureAttributeIndexIterator(scanner: Scanner,
                                      ofilter: Option[Filter],
                                      range: AccRange) {
    val opts = ofilter.map { f => DEFAULT_FILTER_PROPERTY_NAME -> ECQL.toCQL(f)}.toMap

    if(opts.nonEmpty) {
      val cfg = new IteratorSetting(iteratorPriority_AttributeIndexFilteringIterator,
        "attrIndexFilter",
        classOf[AttributeIndexFilteringIterator].getCanonicalName,
        opts)

      configureFeatureType(cfg, featureType)
      scanner.addScanIterator(cfg)
    }

    logger.trace(s"Attribute Scan Range: ${range.toString}")
    scanner.setRange(range)
  }

  def filterListAsAnd(filters: Seq[Filter]): Option[Filter] = filters match {
    case Nil => None
    case _ => Some(ff.and(filters))
  }

  def stIdxQuery(acc: AccumuloConnectorCreator,
                 query: Query,
                 filterVisitor: FilterToAccumulo,
                 output: ExplainerOutputType) = {
    output(s"Scanning ST index table for feature type ${featureType.getTypeName}")

    val spatial = filterVisitor.spatialPredicate
    val temporal = filterVisitor.temporalPredicate

    // TODO: Select only the geometry filters which involve the indexed geometry type.
    // https://geomesa.atlassian.net/browse/GEOMESA-200
    // Simiarly, we should only extract temporal filters for the index date field.
    val (geomFilters, otherFilters) = partitionGeom(query.getFilter)
    val (temporalFilters, ecqlFilters: Seq[Filter]) = partitionTemporal(otherFilters)

    val tweakedEcqlFilters = ecqlFilters.map(tweakFilter)

    val ecql = filterListAsAnd(tweakedEcqlFilters).map(ECQL.toCQL)

    output(s"The geom filters are $geomFilters.\nThe temporal filters are $temporalFilters.")

    // standardize the two key query arguments:  polygon and date-range
    val poly = netPolygon(spatial)
    val interval = netInterval(temporal)

    // figure out which of our various filters we intend to use
    // based on the arguments passed in
    val filter = buildFilter(poly, interval)

    val ofilter = filterListAsAnd(geomFilters ++ temporalFilters)
    if(ofilter.isEmpty) logger.warn(s"Querying Accumulo without ST filter.")

    val oint  = IndexSchema.somewhen(interval)

    // set up row ranges and regular expression filter
    val bs = acc.createSTIdxScanner(featureType)

    planQuery(bs, filter, output)

    output("Configuring batch scanner for ST table: \n" +
      s"  Filter ${query.getFilter}\n" +
      s"  STII Filter: ${ofilter.getOrElse("No STII Filter")}\n" +
      s"  Interval:  ${oint.getOrElse("No interval")}\n" +
      s"  Filter: ${Option(filter).getOrElse("No Filter")}\n" +
      s"  ECQL: ${Option(ecql).getOrElse("No ecql")}\n" +
      s"Query: ${Option(query).getOrElse("no query")}.")

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    //access to query is up here.
    iteratorConfig.iterator match {
      case IndexOnlyIterator  =>
        val transformedSFType = transformedSimpleFeatureType(query).getOrElse(featureType)
        configureIndexIterator(bs, ofilter, query, transformedSFType)
      case SpatioTemporalIterator =>
        val isDensity = query.getHints.containsKey(DENSITY_KEY)
        configureSpatioTemporalIntersectingIterator(bs, ofilter, featureType, isDensity)
    }

    if (iteratorConfig.useSFFI) {
      configureSimpleFeatureFilteringIterator(bs, featureType, ecql, query, poly)
    }

    // NB: Since we are (potentially) gluing multiple batch scanner iterators together,
    //  we wrap our calls in a SelfClosingBatchScanner.
    SelfClosingBatchScanner(bs)
  }

  // Let's handle special cases.
  def tweakFilter(filter: Filter) = {
    filter match {
      case dw: DWithin => rewriteDwithin(dw)
      case _ => filter
    }
  }

  // Rewrites a Dwithin (assumed to express distance in meters) in degrees.
  def rewriteDwithin(op: DWithin): Filter = {
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val startPoint = e2.evaluate(null, classOf[Point])
    val distance = op.getDistance
    val distanceDegrees = GeometryUtils.distanceDegrees(startPoint, distance)

    ff.dwithin(
      op.getExpression1,
      ff.literal(startPoint),
      distanceDegrees,
      "meters")
  }

  def configureFeatureEncoding(cfg: IteratorSetting) =
    cfg.addOption(FEATURE_ENCODING, featureEncoder.getName)

  def configureFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  // returns the SimpleFeatureType for the query's transform
  def transformedSimpleFeatureType(query: Query): Option[SimpleFeatureType] = {
    Option(query.getHints.get(TRANSFORM_SCHEMA)).map {_.asInstanceOf[SimpleFeatureType]}
  }

  // store transform information into an Iterator's settings
  def configureTransforms(query:Query,cfg: IteratorSetting) =
    for {
      transformOpt  <- Option(query.getHints.get(TRANSFORMS))
      transform     = transformOpt.asInstanceOf[String]
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM, transform)
      sfType        <- transformedSimpleFeatureType(query)
      encodedSFType = SimpleFeatureTypes.encodeType(sfType)
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM_SCHEMA, encodedSFType)
    } yield Unit

  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(bs: BatchScanner, regex: String) {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    bs.addScanIterator(cfg)
  }

  // returns an iterator over [key,value] pairs where the key is taken from the index row and the value is a SimpleFeature,
  // which is either read directory from the data row  value or generated from the encoded index row value
  // -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureIndexIterator(bs: BatchScanner,
                             filter: Option[Filter],
                             query: Query,
                             featureType: SimpleFeatureType) {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),classOf[IndexIterator])
    IndexIterator.setOptions(cfg, schema, filter)
    configureFeatureType(cfg, featureType)
    configureFeatureEncoding(cfg)
    bs.addScanIterator(cfg)
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(bs: BatchScanner,
                                                  filter: Option[Filter],
                                                  featureType: SimpleFeatureType,
                                                  isDensity: Boolean) {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),
      classOf[SpatioTemporalIntersectingIterator])
    SpatioTemporalIntersectingIterator.setOptions(cfg, schema, filter)
    configureFeatureType(cfg, featureType)
    if (isDensity) cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    bs.addScanIterator(cfg)
  }
  // assumes that it receives an iterator over data-only entries, and aggregates
  // the values into a map of attribute, value pairs
  def configureSimpleFeatureFilteringIterator(bs: BatchScanner,
                                              simpleFeatureType: SimpleFeatureType,
                                              ecql: Option[String],
                                              query: Query,
                                              poly: Polygon = null) {

    val density: Boolean = query.getHints.containsKey(DENSITY_KEY)

    val clazz =
      if(density) classOf[DensityIterator]
      else classOf[SimpleFeatureFilteringIterator]

    val cfg = new IteratorSetting(iteratorPriority_SimpleFeatureFilteringIterator,
      "sffilter-" + randomPrintableString(5),
      clazz)

    cfg.addOption(DEFAULT_SCHEMA_NAME, schema)
    configureFeatureEncoding(cfg)
    configureTransforms(query,cfg)
    configureFeatureType(cfg, simpleFeatureType)
    ecql.foreach(SimpleFeatureFilteringIterator.setECQLFilter(cfg, _))

    if(density) {
      val width = query.getHints.get(WIDTH_KEY).asInstanceOf[Integer]
      val height =  query.getHints.get(HEIGHT_KEY).asInstanceOf[Integer]
      DensityIterator.configure(cfg, poly, width, height)
    }

    bs.addScanIterator(cfg)
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def planQuery(bs: BatchScanner, filter: KeyPlanningFilter, output: ExplainerOutputType): BatchScanner = {
    output(s"Planning query/configurating batch scanner: $bs")
    val keyPlan = keyPlanner.getKeyPlan(filter, output)
    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }
    bs.setRanges(accRanges)

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    keyPlan.toRegex match {
      case KeyRegex(regex) => configureRowRegexIterator(bs, regex)
      case _ => // do nothing
    }

    // if you have a list of distinct column-family entries, fetch them
    columnFamilies match {
      case KeyList(keys) => {
        output(s"Settings ${keys.size} col fams: $keys.")
        keys.foreach { cf =>
          bs.fetchColumnFamily(new Text(cf))
        }
      }
      case _ => // do nothing
    }

    bs
  }
}
