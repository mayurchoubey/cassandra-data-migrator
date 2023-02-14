package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class CopyPKJobSession extends AbstractJobSession {

	private static CopyPKJobSession copyJobSession;
	public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	protected AtomicLong readCounter = new AtomicLong(0);
	protected AtomicLong missingCounter = new AtomicLong(0);
	protected AtomicLong writeCounter = new AtomicLong(0);

	private AtomicLong correctedMissingCounter = new AtomicLong(0);
	private AtomicLong correctedMismatchCounter = new AtomicLong(0);
	private AtomicLong validCounter = new AtomicLong(0);
	private AtomicLong mismatchCounter = new AtomicLong(0);
	private AtomicLong skippedCounter = new AtomicLong(0);
	private AtomicLong failedRowCounter = new AtomicLong(0);

	private Boolean firstRecord = true;

	protected CopyPKJobSession(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
		super(sourceSession, astraSession, sc, true);
	}

	public static CopyPKJobSession getInstance(CqlSession sourceSession, CqlSession astraSession, SparkConf sc) {
		if (copyJobSession == null) {
			synchronized (CopyPKJobSession.class) {
				if (copyJobSession == null) {
					copyJobSession = new CopyPKJobSession(sourceSession, astraSession, sc);
				}
			}
		}

		return copyJobSession;
	}

	public void getRowAndInsert(List<SplitPartitions.PKRows> rowsList) {
		for (SplitPartitions.PKRows rows : rowsList) {
			rows.pkRows.parallelStream().forEach(row -> {
				readCounter.incrementAndGet();
				String[] pkFields = row.split(" %% ");
				int idx = 0;
				BoundStatement bspk = sourceSelectStatement.bind().setConsistencyLevel(readConsistencyLevel);
				for (MigrateDataType tp : idColTypes) {
					bspk = bspk.set(idx, convert(tp.typeClass, pkFields[idx]), tp.typeClass);
					idx++;
				}
				Row pkRow = sourceSession.execute(bspk).one();
				if (null == pkRow) {
					missingCounter.incrementAndGet();
					logger.error("Could not find row with primary-key: {}", row);
					return;
				}
				ResultSet astraWriteResultSet = astraSession.execute(bindInsert(astraInsertStatement, pkRow, null));
				writeCounter.incrementAndGet();
				if (readCounter.get() % printStatsAfter == 0) {
					printCounts(false);
				}
			});
		}

		printCounts(true);
	}

	@SuppressWarnings("unchecked")
	public void getRowAndDiff(List<SplitPartitions.PKRows> rowsList) {
		for (SplitPartitions.PKRows rows : rowsList) {
			rows.pkRows.parallelStream().forEach(row -> {
				readCounter.incrementAndGet();
				String[] pkFields = row.split(" %% ");
				int idx = 0;
				BoundStatement bspk = sourceSelectStatement.bind();
				try {
					for (MigrateDataType tp : idColTypes) {
						bspk = bspk.set(idx, convertNew(tp.typeClass, pkFields[idx]), tp.typeClass);
						idx++;
					}
				} catch (Exception e) {
					logger.error("Error occurred while type conversion {}", e);
					throw new RuntimeException("Error occurred while type conversion" + e);
				}
				int maxAttempts = maxRetriesRowFailure;
				Row sourceRow = null;
				int diffAttempt = 0;
				for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {
					try {
						sourceRow = sourceSession.execute(bspk).one();
						if (sourceRow != null) {
							Row astraRow = astraSession.execute(selectFromAstra(astraSelectStatement, sourceRow)).one();
							diffAttempt++;
							diff(sourceRow, astraRow, diffAttempt);
						} else {
							logger.error("Could not find row with primary-key: {} on source", row);
						}
						retryCount = maxAttempts;
					} catch (Exception e) {
						logger.error("Could not find row with primary-key: {} retry# {}", row, retryCount, e);
						if (retryCount == maxAttempts) {
							logFailedRecordInFile(sourceRow);
						}
					}
				}
			});
		}
		printValidationCounts(true);
	}

	private void diff(Row sourceRow, Row astraRow, int diffAttempt) {
		if (astraRow == null) {
			if (diffAttempt == 1) {
				missingCounter.incrementAndGet();
				logger.info("Missing target row found for key: {}", getKey(sourceRow));
			}
			astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
			correctedMissingCounter.incrementAndGet();
			logger.info("Inserted missing row in target: {}", getKey(sourceRow));
		} else {
			String diffData = isDifferent(sourceRow, astraRow);
			if (!diffData.isEmpty()) {
				if (diffAttempt == 1) {
					mismatchCounter.incrementAndGet();
					logger.info("Mismatch row found for key: {} Mismatch: {}", getKey(sourceRow), diffData);
				}
				if (isCounterTable) {
					astraSession.execute(bindInsert(astraInsertStatement, sourceRow, astraRow));
				} else {
					astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
				}
				correctedMismatchCounter.incrementAndGet();
				logger.info("Updated mismatch row in target: {}", getKey(sourceRow));
			} else {
				validCounter.incrementAndGet();
			}
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

	public void printValidationCounts(boolean isFinal) {
		String msg = "ThreadID: " + Thread.currentThread().getId();
		if (isFinal) {
			logger.info(
					"################################################################################################");

			logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
			logger.info("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
			logger.info("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
			logger.info("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
			logger.info("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
			logger.info("{} Skipped Record Count: {}", msg, skippedCounter.get());
			logger.info("{} Failed row Count: {}", msg, failedRowCounter.get());
			logger.info("{} Valid Record Count: {}", msg, validCounter.get());
		}

		logger.debug("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
		logger.debug("{} Mismatch Record Count: {}", msg, mismatchCounter.get());
		logger.debug("{} Corrected Mismatch Record Count: {}", msg, correctedMismatchCounter.get());
		logger.debug("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
		logger.debug("{} Corrected Missing Record Count: {}", msg, correctedMissingCounter.get());
		logger.debug("{} Skipped Record Count: {}", msg, skippedCounter.get());
		logger.debug("{} Failed row Count: {}", msg, failedRowCounter.get());
		logger.info("{} Valid Record Count: {}", msg, validCounter.get());

		if (isFinal) {
			logger.info(
					"################################################################################################");
		}
	}

	public void printCounts(boolean isFinal) {
		if (isFinal) {
			logger.info(
					"################################################################################################");
		}
		logger.info("ThreadID: {} Read Record Count: {}", Thread.currentThread().getId(), readCounter.get());
		logger.info("ThreadID: {} Missing Record Count: {}", Thread.currentThread().getId(), missingCounter.get());
		logger.info("ThreadID: {} Inserted Record Count: {}", Thread.currentThread().getId(), writeCounter.get());
		if (isFinal) {
			logger.info(
					"################################################################################################");
		}
	}

	private Object convert(Class<?> targetType, String text) {
		PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
		editor.setAsText(text);
		return editor.getValue();
	}

	private Object convertNew(Class<?> targetType, String text) {
		String className = targetType.getSimpleName();
		switch (className) {
		case "Instant":
			return Instant.parse(text);
		case "ByteBuffer":
			return ByteBuffer.wrap(text.getBytes());
		case "UUID":
			return UUID.fromString(text);
		case "BigDecimal":
			return new BigDecimal(text);
		case "LocalDate":
			return LocalDate.parse(text);
		case "BigInteger":
			return new BigInteger(text);
		default:
			PropertyEditor editor = PropertyEditorManager.findEditor(targetType);
			editor.setAsText(text);
			return editor.getValue();
		}
	}

}