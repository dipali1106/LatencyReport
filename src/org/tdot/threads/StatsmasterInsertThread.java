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

public class StatsmasterInsertThread extends Thread {
	public boolean isRunning = true;
	public boolean serviceStatus = false;
	String subquery = "INSERT INTO `smpp_statsmaster`( `UserId`, `OperatorName`, `CircleName`, `ProcessDate`, `HourNum`, `DayNum`, `MonthNum`, `YearNum`, `ReceivedSmsCount`, `ReceivedSmsCredit`,"
			+ " `SentSmsCount`, `SentSmsCredit`, `DeliveredSmsCount`, `DeliveredSmsCredit`, `FailedSmsCount`, `FailedSmsCredit`, `LatencyLevel1`, `LatencyLevel2`, `LatencyLevel3`, "
			+ "`LatencyLevel4`,   `CancelledSmsCount`, `CancelledSmsCredit`, `FailedLatencyLevel1`, `FailedLatencyLevel2`, `FailedLatencyLevel3`, `FailedLatencyLevel4`) VALUES(";
	StringBuilder sb = new StringBuilder();
	public AtomicInteger processCount = new AtomicInteger(0);
	int totalCon = Integer.parseInt(Constants.mapConInfo.get("TotalConnection"));
	private int poolSize = totalCon;
	private int n = 0;
	private int chunkSize = Constants.mapConInfo.get("ChunkSize")!=null? Integer.parseInt(Constants.mapConInfo.get("ChunkSize")):(int)100;
	int dataCount = 0;
	int waitCount = 0;
	int receivedCount;
	int receivedCredit;
	int failedCount;
	int failedCredit;
	int deliveredCount;
	int deliveredCredit;
	int sentCount;
	int sentCredit;
	int cancelledCount;
	int cancelledCredit;
	ConcurrentLinkedQueue<SmppStatsmaster> queryMap = null;
	int total = 0;
	public void run() {
		System.out.println("StatsmasterInsertThread thread started");
		while (isRunning) {
			if(serviceStatus) {
				isRunning =false;
			}
			if (Constants.statsDataBucket.size() > 0 && processCount.get() <= poolSize) {
				try {
					for (Map.Entry<String, SmppStatsmaster> delPair : Constants.statsDataBucket.entrySet()) {
						if (delPair.getValue().getReceivedSmsCount().get() > 0) {
							SmppStatsmaster obj =delPair.getValue();
							waitCount=0;							
							if (processCount.get() > poolSize + 3) {
								break;
							}
								dataCount++;
								if (sb.length() == 0) {
									queryBuilder(sb,obj, true);
									
									
									queryMap = new ConcurrentLinkedQueue<SmppStatsmaster>();
								} else {
									queryBuilder(sb,obj, false);
									
								}
								
								total++;
								receivedCount = delPair.getValue().getReceivedSmsCount().get();
								receivedCredit = delPair.getValue().getReceivedSmsCredit().get();
								sentCount =	delPair.getValue().getSentSmsCount().get();
								sentCredit = delPair.getValue().getSentSmsCredit().get();
								cancelledCount = delPair.getValue().getCancelledSmsCount().get();
								cancelledCredit = delPair.getValue().getCancelledSmsCredit().get();
								deliveredCount =  delPair.getValue().getDeliveredSmsCount().get();
								deliveredCredit =  delPair.getValue().getDeliveredSmsCredit().get();
								failedCount = delPair.getValue().getFailedSmsCount().get();
								failedCredit = delPair.getValue().getCancelledSmsCredit().get();
								delPair.getValue().getReceivedSmsCount().addAndGet(-receivedCount);
								delPair.getValue().getReceivedSmsCredit().addAndGet(-receivedCredit);
								delPair.getValue().getSentSmsCount().addAndGet(-sentCount);
								delPair.getValue().getSentSmsCredit().addAndGet(-sentCredit);
								delPair.getValue().getCancelledSmsCount().addAndGet(-cancelledCount);
								delPair.getValue().getCancelledSmsCredit().addAndGet(-cancelledCredit);
								delPair.getValue().getDeliveredSmsCount().addAndGet(-deliveredCount);
								delPair.getValue().getDeliveredSmsCredit().addAndGet(-deliveredCredit);
								delPair.getValue().getLatencyLevel1().addAndGet(-delPair.getValue().getLatencyLevel1().get());
								delPair.getValue().getLatencyLevel2().addAndGet(-delPair.getValue().getLatencyLevel2().get());
								delPair.getValue().getLatencyLevel3().addAndGet(-delPair.getValue().getLatencyLevel3().get());
								delPair.getValue().getLatencyLevel4().addAndGet(-delPair.getValue().getLatencyLevel4().get());
								delPair.getValue().getFailedSmsCount().addAndGet(-failedCount);
								delPair.getValue().getFailedSmsCredit().addAndGet(-failedCredit);
								delPair.getValue().getFailedLatencyLevel1().addAndGet(-delPair.getValue().getFailedLatencyLevel1().get());
								delPair.getValue().getFailedLatencyLevel2().addAndGet(-delPair.getValue().getFailedLatencyLevel2().get() );
								delPair.getValue().getFailedLatencyLevel3().addAndGet(-delPair.getValue().getFailedLatencyLevel3().get());
								delPair.getValue().getFailedLatencyLevel4().addAndGet(-delPair.getValue().getFailedLatencyLevel4().get());								
								queryMap.add(obj);
								if (dataCount % chunkSize == 0) {	//System.out.println(sb.toString());								
									if (n >= Constants.dbConNum)n = 0;
									dataCount = 0;									
									Constants.executor.execute(InsertIntoStatsMaster(new ConcurrentLinkedQueue<SmppStatsmaster>(queryMap),sb.toString(), 0, n));
									sb.setLength(0);
									queryMap.clear();
									processCount.getAndIncrement();
									n++;
								}
							
						} else {
							Thread.sleep(500);

						}
					}

					if (dataCount > 0) {System.out.println(sb.toString());
						// if(n>=poolSize) n=5;
						if (n >= Constants.dbConNum)
							n = 0;
						dataCount = 0;
						Constants.executor.execute(InsertIntoStatsMaster(new ConcurrentLinkedQueue<SmppStatsmaster>(queryMap),sb.toString(), 0, n));
						sb.setLength(0);
						queryMap.clear();
						processCount.getAndIncrement();
						n++;
					}
				} catch (InterruptedException ie) {

				} catch (Exception e) {
					Constants.consolelog.add(new StringBuilder()
							.append("Exception Position 1 Occured On StatsmasterInsertThread.class (run Method) : ")
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
			if (Constants.tableBucket.size()==0 ) {
				System.out.println("Here");
					
				for (Map.Entry<String, SmppStatsmaster> delPair : Constants.statsDataBucket.entrySet()) {
					if (delPair.getValue().getReceivedSmsCount().get() == 0) {
						isRunning = false;
					}else {
						isRunning = true;
						break;
					}
				}
				
			}

		}
		System.out.println(total + "  data inserted. Completed");
	}
	
	public Runnable InsertIntoStatsMaster(final ConcurrentLinkedQueue<SmppStatsmaster> queryMap, final String queryStr, final int loopcount, final int x) {
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
							Constants.TotalBufferedRecord.addAndGet(-queryMap.size());
							Constants.TotalProcessedRecord.addAndGet(queryMap.size());
							stmt.close();
							processCount.getAndDecrement();
							queryMap.clear();
						}
					}
					else {
						
					}

				} catch (SQLTransactionRollbackException st) {
					System.out.println("Step -3");
					// st.printStackTrace();
					try {
						Thread.sleep(1000);
						Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr, ++count, x));
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (SQLTimeoutException sto) {
					System.out.println("Step -4");
					// sto.printStackTrace();
					try {
						Thread.sleep(1000);
						Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr, ++count, x));
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
							Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
						} else {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
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
							Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
						} else {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
						}
					} catch (InterruptedException e) {
						processCount.getAndDecrement();
						e.printStackTrace();
					}
				} catch (SQLIntegrityConstraintViolationException ev) {
					System.out.println("Step -7");
					try {
						processCount.getAndDecrement();
						Constants.errorBucket.addAll(queryMap);
						
						queryMap.clear();
						Constants.consolelog.add(new StringBuilder().append(
								"MySQLIntegrityConstraintViolationException Position 1 Occured On StatsmasterInsertThread.class (InsertIntoStatsMaster Runnable) : ")
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
					if (msql.getMessage().contains("smpp_statsmaster")
							|| msql.getMessage().contains("doesn't exist")) {
						try {
							processCount.getAndDecrement();
							Constants.errorBucket.addAll(queryMap);
							
							queryMap.clear();
						} catch (Exception e) {
							processCount.getAndDecrement();
							e.printStackTrace();
						}
					} else {
						// msql.printStackTrace();
						processCount.getAndDecrement();
						Constants.errorBucket.addAll(queryMap);
						
						queryMap.clear();
					}
					Constants.consolelog.add(new StringBuilder().append(
							"MySQLSyntaxErrorException Position 2 Occured On StatsmasterInsertThread.class (InsertIntoStatsMaster Runnable) : ")
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
								Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
							} else {
								Thread.sleep(1000);
								Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
							}
						} catch (InterruptedException e) {
							processCount.getAndDecrement();
							e.printStackTrace();
						}
					} else {
						System.out.println("Step 11");

						processCount.getAndDecrement();
						Constants.errorBucket.addAll(queryMap);
						
						queryMap.clear();
						Constants.consolelog.add(new StringBuilder().append(
								"SQLException Position 3 Occured On StatsmasterInsertThread.class (InsertIntoStatsMaster Runnable) : ")
								.append(sql.getMessage()).toString());
					}
				} catch (NullPointerException np) {
					System.out.println("Step -12");
					// np.printStackTrace();
					try {
						Constants.consolelog.add(new StringBuilder().append(
								"NullPointerException Position 4 Occured On StatsmasterInsertThread.class (InsertIntoStatsMaster Runnable) : ")
								.append(np.getMessage()).toString());
						status = DbConnection.getDataDbSqlConnection(x);
						if (status) {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr, ++count, x));
						} else {
							Thread.sleep(1000);
							Constants.executor.execute(InsertIntoStatsMaster(queryMap, queryStr,  ++count, x));
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
							"Exception Position 5 Occured On StatsmasterInsertThread.class (InsertIntoStatsMaster Runnable) : ")
							.append(e.getMessage()).toString());
				}
			}

		};

	}

	
	public StringBuilder queryBuilder(StringBuilder sb, SmppStatsmaster obj , boolean flag) {
		if(flag) {
			sb.append(subquery ).append(obj.getUserId()).append(",'").append(obj.getOperatorName()).append("','")
			.append(obj.getCircleName()).append("','").append(obj.getProcessDate()).append("',")
			.append(obj.getHourNum()).append(",").append(obj.getDayNum()).append(",")
			.append(obj.getMonthNum()).append(",").append(obj.getYearNum()).append(",")
			.append(obj.getReceivedSmsCount().get()).append(",").append(obj.getReceivedSmsCredit().get()).append(",")
			.append(obj.getSentSmsCount().get()).append(",").append(obj.getSentSmsCredit().get()).append(",")
			.append(obj.getDeliveredSmsCount().get()).append(",").append(obj.getDeliveredSmsCredit().get()).append(",")
			.append(obj.getFailedSmsCount().get()).append(",").append(obj.getFailedSmsCredit().get()).append(",")
			.append(obj.getLatencyLevel1().get()).append(",").append(obj.getLatencyLevel2().get()).append(",")
			.append(obj.getLatencyLevel3().get()).append(",").append(obj.getLatencyLevel4().get()).append(",")
			.append(obj.getCancelledSmsCount().get()).append(",").append(obj.getCancelledSmsCredit().get()).append(",")
			.append(obj.getFailedLatencyLevel1().get()).append(",").append(obj.getFailedLatencyLevel2().get()).append(",")
			.append(obj.getFailedLatencyLevel3().get()).append(",").append(obj.getFailedLatencyLevel4()).append(")");
		}
		else {
			sb.append(", ('").append(obj.getUserId()).append(",'").append(obj.getOperatorName()).append("','")
			.append(obj.getCircleName()).append("','").append(obj.getProcessDate()).append("',")
			.append(obj.getHourNum()).append(",").append(obj.getDayNum()).append(",")
			.append(obj.getMonthNum()).append(",").append(obj.getYearNum()).append(",")
			.append(obj.getReceivedSmsCount().get()).append(",").append(obj.getReceivedSmsCredit().get()).append(",")
			.append(obj.getSentSmsCount().get()).append(",").append(obj.getSentSmsCredit().get()).append(",")
			.append(obj.getDeliveredSmsCount().get()).append(",").append(obj.getDeliveredSmsCredit().get()).append(",")
			.append(obj.getFailedSmsCount().get()).append(",").append(obj.getFailedSmsCredit().get()).append(",")
			.append(obj.getLatencyLevel1().get()).append(",").append(obj.getLatencyLevel2().get()).append(",")
			.append(obj.getLatencyLevel3().get()).append(",").append(obj.getLatencyLevel4().get()).append(",")
			.append(obj.getCancelledSmsCount().get()).append(",").append(obj.getCancelledSmsCredit().get()).append(",")
			.append(obj.getFailedLatencyLevel1().get()).append(",").append(obj.getFailedLatencyLevel2().get()).append(",")
			.append(obj.getFailedLatencyLevel3().get()).append(",").append(obj.getFailedLatencyLevel4()).append(")");
			
		}
		return sb;
	}


}
