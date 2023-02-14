package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory

object DiffDataFailedRowsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigrateRowsFromFile App")

  migrateTable(sourceConnection, destinationConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector) = {
    val listOfPKRows = SplitPartitions.getFailedRowPartsFromFile(splitSize, rowFailureFileSizeLimit, failedRowsFile)
    logger.info("PARAM Calculated -- Number of PKRows: " + listOfPKRows.size())

    sourceConnection.withSessionDo(sourceSession =>
      destinationConnection.withSessionDo(destinationSession =>
        CopyPKJobSession.getInstance(sourceSession, destinationSession, sc)
          .getRowAndDiff(listOfPKRows)))
  
  }

}
