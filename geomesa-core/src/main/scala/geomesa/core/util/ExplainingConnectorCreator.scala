package geomesa.core.util

import geomesa.core.data.{AccumuloConnectorCreator, AccumuloDataStore}
import geomesa.core.index.ExplainerOutputType
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Scanner, BatchScanner}
import org.opengis.feature.simple.SimpleFeatureType


class ExplainingConnectorCreator(output: ExplainerOutputType) extends AccumuloConnectorCreator {
  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   *
   * @param numThreads number of threads for the BatchScanner
   */
  override def createSpatioTemporalIdxScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = new ExplainingBatchScanner(output)

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   */
  override def createSTIdxScanner(sft: SimpleFeatureType): BatchScanner = new ExplainingBatchScanner(output)

  /**
   * Create a Scanner for the Attribute Table (Inverted Index Table)
   */
  override def createAttrIdxScanner(sft: SimpleFeatureType): Scanner = new ExplainingScanner(output)

  /**
   * Create a BatchScanner to retrieve only Records (SimpleFeatures)
   */
  override def createRecordScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = new ExplainingBatchScanner(output)

  override def catalogTableFormat(sft: SimpleFeatureType): Boolean = true
}
