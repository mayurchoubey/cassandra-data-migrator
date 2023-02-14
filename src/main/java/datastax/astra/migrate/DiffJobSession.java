package datastax.astra.migrate;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

public class DiffJobSession extends CopyJobSession {

	private static DiffJobSession diffJobSession;
	public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	protected Boolean autoCorrectMissing = false;
	protected Boolean autoCorrectMismatch = false;
	private AtomicLong readCounter = new AtomicLong(0);
	private AtomicLong mismatchCounter = new AtomicLong(0);
	private AtomicLong missingCounter = new AtomicLong(0);
	private AtomicLong correctedMissingCounter = new AtomicLong(0);
	private AtomicLong correctedMismatchCounter = new AtomicLong(0);
	private AtomicLong validCounter = new AtomicLong(0);
	private AtomicLong skippedCounter = new AtomicLong(0);
	private AtomicLong failedRowCounter = new AtomicLong(0);

	private DiffJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
		super(sourceSession, astraSession, sc);

		autoCorrectMissing = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.missing", "false"));
		logger.info("PARAM -- Autocorrect Missing: {}", autoCorrectMissing);

		autoCorrectMismatch = Boolean
				.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.mismatch", "false"));
		logger.info("PARAM -- Autocorrect Mismatch: {}", autoCorrectMismatch);
	}

	public static DiffJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sparkConf) {
		if (diffJobSession == null) {
			synchronized (DiffJobSession.class) {
				if (diffJobSession == null) {
					diffJobSession = new DiffJobSession(sourceSession, astraSession, sparkConf);
				}
			}
		}

		return diffJobSession;
	}

	public void getDataAndDiff(BigInteger min, BigInteger max) {
		logger.info("ThreadID: {} Processing min: {} max: {}", Thread.currentThread().getId(), min, max);
		int maxAttempts = maxRetries;
		for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

			try {
				// cannot do batching if the writeFilter is greater than 0
				ResultSet resultSet = sourceSession.execute(sourceSelectStatement
						.bind(hasRandomPartitioner ? min : min.longValueExact(),
								hasRandomPartitioner ? max : max.longValueExact())
						.setConsistencyLevel(readConsistencyLevel).setPageSize(fetchSizeInRows));

				Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap = new HashMap<Row, CompletionStage<AsyncResultSet>>();
				StreamSupport.stream(resultSet.spliterator(), false).forEach(srcRow -> {
					readLimiter.acquire(1);
					// do not process rows less than writeTimeStampFilter
					if (!(writeTimeStampFilter && (getLargestWriteTimeStamp(srcRow) < minWriteTimeStampFilter
							|| getLargestWriteTimeStamp(srcRow) > maxWriteTimeStampFilter))) {
						if (readCounter.incrementAndGet() % printStatsAfter == 0) {
							printCounts(false);
						}

						CompletionStage<AsyncResultSet> targetRowFuture = astraSession
								.executeAsync(selectFromAstra(astraSelectStatement, srcRow));
						srcToTargetRowMap.put(srcRow, targetRowFuture);
						if (srcToTargetRowMap.size() > fetchSizeInRows) {
							diffAndClear(srcToTargetRowMap);
						}
					} else {
						readCounter.incrementAndGet();
						skippedCounter.incrementAndGet();
					}
				});
				diffAndClear(srcToTargetRowMap);
				retryCount = maxAttempts;
			} catch (Exception e) {
				logger.error("Error occurred retry#: {}", retryCount, e);
				logger.error("Error with PartitionRange -- ThreadID: {} Processing min: {} max: {} -- Retry# {}",
						Thread.currentThread().getId(), min, max, retryCount);
				if (retryCount == maxAttempts) {
					logFailedPartitionsInFile(min, max);
				}
			}
		}

	}

	private void logFailedPartitionsInFile(BigInteger min, BigInteger max) {
		try {
			Util.FileAppend(tokenRangeExceptionDir, exceptionFileName, min + "," + max);
		} catch (Exception ee) {
			logger.error("Error occurred while writing to token range file min: {} max: {}", min, max, ee);
		}
	}

	private void logFailedRecordInFile(Row sourceRow) {
		try {
			failedRowCounter.getAndIncrement();
			Util.FileAppend(rowExceptionDir, exceptionFileName, getKey(sourceRow));
			logger.error("Failed to validate row: {} after {} retry.", getKey(sourceRow));
		} catch (Exception exp) {
			logger.error("Error occurred while writing to key {} to file ", getKey(sourceRow), exp);
		}
	}

	private void diffAndClear(Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap) {
		for (Row srcRow : srcToTargetRowMap.keySet()) {
			int maxAttempts = maxRetriesRowFailure;
			for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {
				try {
					Row targetRow = srcToTargetRowMap.get(srcRow).toCompletableFuture().get().one();
					diff(srcRow, targetRow, retryCount);
					retryCount = maxAttempts;
				} catch (Exception e) {
					logger.error("Could not perform diff for Key: {} -- Retry# {}", getKey(srcRow), retryCount, e);
					if (retryCount == maxAttempts) {
						logFailedRecordInFile(srcRow);
					}
				}
			}
		}
		srcToTargetRowMap.clear();
	}

	public synchronized void printCounts(boolean isFinal) {
		String msg = "ThreadID: " + Thread.currentThread().getId();
		if (isFinal) {
			msg += " Final";
			logger.info(
					"################################################################################################");
			logger.info("{} Read Record Count: {}", msg, readCounter.get());
			logger.info("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
			logger.info("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
			logger.info("{} Missing Record Count: {}", msg, missingCounter.get());
			logger.info("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
			logger.info("{} Valid Record Count: {}", msg, validCounter.get());
			logger.info("{} Skipped Record Count: {}", msg, skippedCounter.get());
			logger.info("{} Failed row Count: {}", msg, failedRowCounter.get());
		}
		logger.debug("{} Read Record Count: {}", msg, readCounter.get());
		logger.debug("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
		logger.debug("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
		logger.debug("{} Missing Record Count: {}", msg, missingCounter.get());
		logger.debug("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
		logger.debug("{} Valid Record Count: {}", msg, validCounter.get());
		logger.debug("{} Skipped Record Count: {}", msg, skippedCounter.get());
		logger.debug("{} Failed row Count: {}", msg, failedRowCounter.get());
		if (isFinal) {
			logger.info(
					"################################################################################################");
		}
	}

	private void diff(Row sourceRow, Row astraRow, int retry) {
		if (astraRow == null) {
			if (retry == 1) {
				missingCounter.incrementAndGet();
				logger.error("Missing target row found for key: {}", getKey(sourceRow));
			}
			if (autoCorrectMissing) {
				astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
				correctedMissingCounter.incrementAndGet();
				logger.error("Inserted missing row in target: {}", getKey(sourceRow));
			}
			return;
		}

		String diffData = isDifferent(sourceRow, astraRow);
		if (!diffData.isEmpty()) {
			if (retry == 1) {
				mismatchCounter.incrementAndGet();
				logger.error("Mismatch row found for key: {} Mismatch: {}", getKey(sourceRow), diffData);
			}
			if (autoCorrectMismatch) {
				if (isCounterTable) {
					astraSession.execute(bindInsert(astraInsertStatement, sourceRow, astraRow));
				} else {
					if (writeTimeStampCols.isEmpty()) {
						checkAndUpdateForComplexTypeCols(sourceRow, astraRow);
					} else {
						astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
						logger.error("Updated mismatch row in target: {}", getKey(sourceRow));
					}
					correctedMismatchCounter.incrementAndGet();
				}
			}
			return;
		}

		validCounter.incrementAndGet();
	}

	/**
	 * Use Case: Program fetches data from source Cassandra and stores it in memory
	 * and compare one record at a time with target Cassandra. During comparison if
	 * data is updated on both source and target databases on finding diff program
	 * will override old data from source over target. Use Case specific to complex
	 * tables having only set, map, list, UDT etc as column type for non primary
	 * columns.
	 * 
	 * @param sourceRow
	 * @param astraRow
	 * @param diffData
	 */
	private void checkAndUpdateForComplexTypeCols(Row sourceRow, Row astraRow) {
		Row latestRow = sourceSession.execute(selectFromAstra(sourceSelectLatestStatement, sourceRow)).one();
		String diffData = isDifferent(latestRow, astraRow);
		if (!diffData.isEmpty()) {
			logger.error("Mismatch found even after matching with latest record found for key: {} Mismatch: {}",
					getKey(latestRow), diffData);
			astraSession.execute(bindInsert(astraInsertStatement, latestRow, null));
			logger.error("Updated mismatch row in target: {}", getKey(sourceRow));
		} else {
			logger.info("No mismatch after matching with latest record for key: {} ", getKey(latestRow));
		}
	}

	private String isDifferent(Row sourceRow, Row astraRow) {
		StringBuffer diffData = new StringBuffer();
		IntStream.range(0, selectColTypes.size()).parallel().forEach(index -> {
			MigrateDataType dataType = selectColTypes.get(index);
			Object source = getData(dataType, index, sourceRow);
			Object astra = getData(dataType, index, astraRow);

			boolean isDiff = dataType.diff(source, astra);
			if (isDiff) {
				if (dataType.typeClass.equals(UdtValue.class)) {
					String sourceUdtContent = ((UdtValue) source).getFormattedContents();
					String astraUdtContent = ((UdtValue) astra).getFormattedContents();
					if (!sourceUdtContent.equals(astraUdtContent)) {
						diffData.append("(Index: " + index + " Origin: " + sourceUdtContent + " Target: "
								+ astraUdtContent + ") ");
					}
				} else {
					diffData.append("(Index: " + index + " Origin: " + source + " Target: " + astra + ") ");
				}
			}
		});

		return diffData.toString();
	}

}