/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.core.iterators.GeoMesaAggregatingIterator.Result

import scala.languageFeature.implicitConversions
import scala.collection.JavaConverters._
import scalaz.Scalaz._

case class MapAggregatingIteratorResult(mapAttributeName: String,
                                        countMap: Map[String, Int] = Map()) extends Result {
  override def addToFeature(sfb: SimpleFeatureBuilder): Unit =  {
    sfb.add(countMap)
    sfb.add(MapAggregatingIterator.FAKE_GEOMETRY)
  }
}

class MapAggregatingIterator(other: MapAggregatingIterator, env: IteratorEnvironment)
  extends GeoMesaAggregatingIterator[MapAggregatingIteratorResult](other, env) {

  var mapAttribute: String = null

  def this() = this(null, null)

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment): Unit = {

    mapAttribute = options.get(MapAggregatingIterator.MAP_ATTRIBUTE_KEY)

    projectedSFTDef = MapAggregatingIterator.sft(mapAttribute)

    super.init(source, options, env)
  }

  override def handleKeyValue(resultO: Option[MapAggregatingIteratorResult], topSourceKey: Key, topSourceValue: Value): MapAggregatingIteratorResult = {

    val feature = originalDecoder.decode(topSourceValue)
    val currCounts = feature.getAttribute(mapAttribute).asInstanceOf[JMap[String, Int]].asScala.toMap //TODO: may be inefficient, figure out mutable/immutable maps

    val result = resultO.getOrElse(MapAggregatingIteratorResult(mapAttribute))
    result.copy(countMap = result.countMap |+| currCounts)
  }

  def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = new MapAggregatingIterator(this, env)
}

object MapAggregatingIterator extends Logging {

  val MAP_ATTRIBUTE_KEY = "map_attribute"
  def sft(mapAttributeName: String) = s"$mapAttributeName:Map[String,Integer],geom:Point:srid=4326"

  val geomFactory = JTSFactoryFinder.getGeometryFactory

  val FAKE_GEOMETRY = geomFactory.createPoint(new Coordinate(0,0))

  def configure(cfg: IteratorSetting, mapAttribute: String) =
    setMapAttribute(cfg, mapAttribute)

  def setMapAttribute(iterSettings: IteratorSetting, mapAttribute: String): Unit =
    iterSettings.addOption(MAP_ATTRIBUTE_KEY, mapAttribute)

  def getBounds(options: JMap[String, String]): String = options.get(MAP_ATTRIBUTE_KEY)
}

