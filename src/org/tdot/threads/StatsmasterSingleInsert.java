package org.tdot.threads;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.tdot.connection.DbConnection;
import org.tdot.entity.SmppStatsmaster;
import org.tdot.utils.Constants;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;

public class StatsmasterSingleInsert extends Thread {
	public boolean isRunning = true;
	public boolean serviceStatus = false;
	String subquery = "INSERT INTO `smpp_statsmaster`( `UserId`, `OperatorName`, `CircleName`, `ProcessDate`, `HourNum`, `DayNum`, `MonthNum`, `YearNum`, `ReceivedSmsCount`, `ReceivedSmsCredit`,"
			+ " `SentSmsCount`, `SentSmsCredit`, `DeliveredSmsCount`, `DeliveredSmsCredit`, `FailedSmsCount`, `FailedSmsCredit`, `LatencyLevel1`, `LatencyLevel2`, `LatencyLevel3`, `LatencyLevel4`, "
			+ " `RequestSource`, `EntryDateTime`, `CancelledSmsCount`, `CancelledSmsCredit`, `FailedLatencyLevel1`, `FailedLatencyLevel2`, `FailedLatencyLevel3`, `FailedLatencyLevel4`) VALUES(";
	StringBuilder sb = new StringBuilder();
	public AtomicInteger processCount = new AtomicInteger(0);
	int totalCon = Integer.parseInt(Constants.mapConInfo.get("TotalConnection"));
	private int poolSize = totalCon;
	private int n = 0;
	private int chunkSize = Constants.mapConInfo.get("ChunkSize") != null
			? Integer.parseInt(Constants.mapConInfo.get("ChunkSize"))
			: (int) 100;

	int total = 0;

	public void run() {
		System.out.println("StatsmasterInsertThread thread started");
		while (isRunning) {
			if (serviceStatus) {
				isRunning = false;
			}
			if (Constants.errorBucket.size() > 0 && processCount.get() <= poolSize) {
				try {
					for (SmppStatsmaster obj : Constants.errorBucket) {
						if (obj.getReceivedSmsCount().get() > 0) {
							if (processCount.get() > poolSize + 3) {
								break;
							}
							queryBuilder(sb, obj, true);
							if (n >= Constants.dbConNum)
								n = 0;
							Constants.executor.execute(InsertIntoStatsMaster(sb.toString(), 0, n));
							sb.setLength(0);
							processCount.getAndIncrement();
							n++;
						}

					}

				} catch (Exception e) {
					Constants.consolelog.add(new StringBuilder()
							.append("Exception Position 1 Occured On DeliveredInsertThread.class (run Method) : ")
							.append(e.getMessage()).toString());
					e.printStackTrace();
				}

			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (Constants.TotalPending == Constants.TotalProcessedRecord.get()) {

				isRunning = false;

			}

		}
		System.out.println(total + " delivered data inserted. Completed");
	}

	public Runnable InsertIntoStatsMaster(final String queryStr, final int loopcount, final int x) {
		return new Runnable() {
			private Statement stmt = null;
			private int count = 0;
			private boolean status = true;

			@Override
			public void run() {
				try {
					count = loopcount;
					// System.out.println("Connection size"+DefaultBox.cdrConnectionSet.size());
					if (count < 2) {
						if (Constants.dataConnection.get(x) != null) {
							stmt = Constants.dataConnection.get(x).createStatement();
							stmt.execute("SET NAMES utf8");
							// System.out.println(queryStr);
							stmt.execute(queryStr);

							stmt.close();
							processCount.getAndDecrement();
						}
					}

				} catch (SQLTransactionRollbackException st) {
					System.out.println("Step -3");
					// st.printStackTrace();
					try {
						Thread.sleep(1000);
						Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (SQLTimeoutException sto) {
					System.out.println("Step -4");
					// sto.printStackTrace();
					try {
						Thread.sleep(1000);
						Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (SQLNonTransientConnectionException snt) {
					System.out.println("Step -5");
					// snt.printStackTrace();
					try {
						status = DbConnection.getDataDbSqlConnection(x);
						if (status) {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
						} else {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
						}
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (CommunicationsException snt) {
					System.out.println("Step -6");
					// snt.printStackTrace();
					try {
						status = DbConnection.getDataDbSqlConnection(x);
						if (status) {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
						} else {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
						}
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (SQLIntegrityConstraintViolationException ev) {
					System.out.println("Step -7");
					try {
						processCount.getAndDecrement();

						Constants.consolelog.add(new StringBuilder().append(
								"MySQLIntegrityConstraintViolationException Position 1 Occured On StatsmasterSingleInsert.class (InsertIntoStatsMaster Runnable) : ")
								.append(ev.getMessage()).toString());
					} catch (Exception e1) {
						System.out.println("Step -8");

						processCount.getAndDecrement();
						// DefaultBox.totalBufferedRecord.addAndGet(-queryMap.size());
						e1.printStackTrace();
					}
				} catch (SQLSyntaxErrorException msql) {
					System.out.println("Step -9");
					// msql.printStackTrace();
					if (msql.getMessage().contains("smppstatsmaster")
							|| msql.getMessage().contains("doesn't exist")) {
						try {
							processCount.getAndDecrement();
						} catch (Exception e) {
							processCount.getAndDecrement();
							e.printStackTrace();
						}
					} else {
						// msql.printStackTrace();
						processCount.getAndDecrement();
					}
					Constants.consolelog.add(new StringBuilder().append(
							"MySQLSyntaxErrorException Position 2 Occured On StatsmasterSingleInsert.class (InsertIntoStatsMaster Runnable) : ")
							.append(msql.getMessage()).toString());
				} catch (SQLException sql) {
					System.out.println("Step -10");
					System.out.println(queryStr);
					sql.printStackTrace();
					if (sql.getMessage().equalsIgnoreCase("Could not retrieve transaction read-only status from server")
							|| sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed.")
							|| sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed")) {
						try {
							status = DbConnection.getSqlConnection(x);
							if (status) {
								Thread.sleep(1000);
								Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
							} else {
								Thread.sleep(1000);
								Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
							}
						} catch (InterruptedException e) {
							processCount.getAndDecrement();
							e.printStackTrace();
						}
					} else {
						System.out.println("Step 11");

						processCount.getAndDecrement();

						Constants.consolelog.add(new StringBuilder().append(
								"SQLException Position 3 Occured On StatsmasterSingleInsert.class (InsertIntoStatsMaster Runnable) : ")
								.append(sql.getMessage()).toString());
					}
				} catch (NullPointerException np) {
					System.out.println("Step -12");
					// np.printStackTrace();
					try {
						Constants.consolelog.add(new StringBuilder().append(
								"NullPointerException Position 4 Occured On StatsmasterSingleInsert.class (InsertIntoStatsMaster Runnable) : ")
								.append(np.getMessage()).toString());
						status = DbConnection.getDataDbSqlConnection(x);
						if (status) {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
						} else {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryStr, ++count, x));
						}
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (Exception e) {
					System.out.println("Step -13");
					e.printStackTrace();
					processCount.getAndDecrement();
					Constants.consolelog.add(new StringBuilder().append(
							"Exception Position 5 Occured On StatsmasterSingleInsert.class (InsertIntoStatsMaster Runnable) : ")
							.append(e.getMessage()).toString());
				}
			}

		};

	}

	public StringBuilder queryBuilder(StringBuilder sb, SmppStatsmaster obj, boolean flag) {
		if (flag) {
			sb.append(subquery).append(obj.getUserId()).append(",'").append(obj.getOperatorName()).append("','")
					.append(obj.getCircleName()).append("','").append(obj.getProcessDate()).append("',")
					.append(obj.getHourNum()).append(",").append(obj.getDayNum()).append(",").append(obj.getMonthNum())
					.append(",").append(obj.getYearNum()).append(",").append(obj.getReceivedSmsCount().get())
					.append(",").append(obj.getReceivedSmsCredit().get()).append(",")
					.append(obj.getSentSmsCount().get()).append(",").append(obj.getSentSmsCredit().get()).append(",")
					.append(obj.getDeliveredSmsCount().get()).append(",").append(obj.getDeliveredSmsCredit().get())
					.append(",").append(obj.getFailedSmsCount().get()).append(",")
					.append(obj.getFailedSmsCredit().get()).append(",").append(obj.getLatencyLevel1().get()).append(",")
					.append(obj.getLatencyLevel2().get()).append(",").append(obj.getLatencyLevel3().get()).append(",")
					.append(obj.getLatencyLevel4().get()).append(",").append(obj.getCancelledSmsCount().get())
					.append(",").append(obj.getCancelledSmsCredit().get()).append(",")
					.append(obj.getFailedLatencyLevel1().get()).append(",").append(obj.getFailedLatencyLevel2().get())
					.append(",").append(obj.getFailedLatencyLevel3().get()).append(",")
					.append(obj.getFailedLatencyLevel4()).append(")");
		} else {
			sb.append(", ('").append(obj.getUserId()).append(",'").append(obj.getOperatorName()).append("','")
					.append(obj.getCircleName()).append("','").append(obj.getProcessDate()).append("',")
					.append(obj.getHourNum()).append(",").append(obj.getDayNum()).append(",").append(obj.getMonthNum())
					.append(",").append(obj.getYearNum()).append(",").append(obj.getReceivedSmsCount().get())
					.append(",").append(obj.getReceivedSmsCredit().get()).append(",")
					.append(obj.getSentSmsCount().get()).append(",").append(obj.getSentSmsCredit().get()).append(",")
					.append(obj.getDeliveredSmsCount().get()).append(",").append(obj.getDeliveredSmsCredit().get())
					.append(",").append(obj.getFailedSmsCount().get()).append(",")
					.append(obj.getFailedSmsCredit().get()).append(",").append(obj.getLatencyLevel1().get()).append(",")
					.append(obj.getLatencyLevel2().get()).append(",").append(obj.getLatencyLevel3().get()).append(",")
					.append(obj.getLatencyLevel4().get()).append(",").append(obj.getCancelledSmsCount().get())
					.append(",").append(obj.getCancelledSmsCredit().get()).append(",")
					.append(obj.getFailedLatencyLevel1().get()).append(",").append(obj.getFailedLatencyLevel2().get())
					.append(",").append(obj.getFailedLatencyLevel3().get()).append(",")
					.append(obj.getFailedLatencyLevel4()).append(")");

		}
		return sb;
	}
}
