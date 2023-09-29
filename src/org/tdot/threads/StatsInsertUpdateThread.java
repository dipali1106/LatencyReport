package org.tdot.threads;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.tdot.connection.DbConnection;
import org.tdot.entity.SmppStatsmaster;
import org.tdot.utils.Constants;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

public class StatsInsertUpdateThread  extends Thread{
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
	int latency1=0;
	int latency2=0;
	int latency3=0;
	int latency4=0;
	int failedlatency1=0;
	int failedlatency2=0;
	int failedlatency3=0;
	int failedlatency4=0;
	int srNo = 0;
	
	
	
	public void run() {
		ResultSet result =null;
		Statement smt=null;
		System.out.println("StatsmasterInsertThread thread started");
		while (isRunning) {
			if(serviceStatus) {
				isRunning =false;
			}
			
			if (Constants.statsDataBucket.size() > 0 && processCount.get() <= poolSize) {
				try {
					for (Map.Entry<String, SmppStatsmaster> delPair : Constants.statsDataBucket.entrySet()) {
						if (delPair.getValue().getReceivedSmsCount().get() > 0) {
							SmppStatsmaster obj = new  SmppStatsmaster(delPair.getValue());
							if (processCount.get() > poolSize + 3) {
								break;
							}
							if(Constants.statDates.contains((obj.getProcessDate()))){
								String query = new StringBuilder().append("SELECT `SrNo` FROM `smpp_statsmaster` WHERE `UserId` = ").append(obj.getUserId()).append(" AND `OperatorName`= '").append(obj.getOperatorName())
										.append("' AND `CircleName`= '").append(obj.getCircleName()).append("' AND `ProcessDate`= '").append(obj.getProcessDate()).append("'  AND `HourNum` = ").append(obj.getHourNum()).toString();
								if (n >= Constants.dbConNum)n = 0;
								smt=Constants.dataConnection.get(n).createStatement();
								result = smt.executeQuery(query);
								if(result.next()) {
									srNo = result.getInt("SrNo");
								}
								result.close();
								receivedCount = obj.getReceivedSmsCount().get();
								Constants.TotalProcessedRecord.addAndGet(receivedCount);
								receivedCredit = obj.getReceivedSmsCredit().get();
								sentCount =	obj.getSentSmsCount().get();
								sentCredit = obj.getSentSmsCredit().get();
								cancelledCount = obj.getCancelledSmsCount().get();
								cancelledCredit = obj.getCancelledSmsCredit().get();
								deliveredCount =  obj.getDeliveredSmsCount().get();
								deliveredCredit =  obj.getDeliveredSmsCredit().get();
								failedCount = obj.getFailedSmsCount().get();
								failedCredit = obj.getCancelledSmsCredit().get();
								if(srNo!= 0) {
									query = new StringBuilder("UPDATE `smpp_statsmaster` SET `ReceivedSmsCount`= `ReceivedSmsCount`+").append(obj.getReceivedSmsCount())
											.append(" ,`ReceivedSmsCredit`= `ReceivedSmsCredit`+ ").append(obj.getReceivedSmsCredit()).append(" ,`SentSmsCount`= `SentSmsCount` +").append(obj.getSentSmsCount())
											.append(" ,`SentSmsCredit`= `SentSmsCredit`+").append(obj.getSentSmsCredit()).append(" ,`DeliveredSmsCount`= `DeliveredSmsCount`+").append(obj.getDeliveredSmsCount())
											.append(" ,`DeliveredSmsCredit`= `DeliveredSmsCredit`+ ").append(obj.getDeliveredSmsCredit()).append(",`FailedSmsCount`= `FailedSmsCount`+").append(obj.getFailedSmsCount())
											.append(",`FailedSmsCredit`= `FailedSmsCredit`+").append(obj.getFailedSmsCredit()).append(" ,`LatencyLevel1`= `LatencyLevel1`+").append(obj.getLatencyLevel1())
											.append(",`LatencyLevel2`= `LatencyLevel2`+").append(obj.getLatencyLevel2()).append(",`LatencyLevel3`= `LatencyLevel3`+").append(obj.getLatencyLevel3())
											.append(",`LatencyLevel4`= `LatencyLevel4`+").append(obj.getLatencyLevel4()).append(",`CancelledSmsCount`= `CancelledSmsCount`+ ").append(obj.getCancelledSmsCount())
											.append(",`CancelledSmsCredit`= `CancelledSmsCredit`+ ").append(obj.getCancelledSmsCredit()).append(",`FailedLatencyLevel1`= `FailedLatencyLevel1`+ ").append(obj.getFailedLatencyLevel1())
											.append(",`FailedLatencyLevel2`= `FailedLatencyLevel2`+ ").append(obj.getFailedLatencyLevel2()).append(",`FailedLatencyLevel3`= `FailedLatencyLevel3`+ ").append(obj.getFailedLatencyLevel3())
											.append(",`FailedLatencyLevel4`= `FailedLatencyLevel4`+ ").append(obj.getFailedLatencyLevel4()).append(" WHERE `UserId` =").append(obj.getUserId()).append(" AND `OperatorName`= '").append(obj.getOperatorName())
											.append("' AND `CircleName`= '").append(obj.getCircleName()).append("' AND `ProcessDate`= '").append(obj.getProcessDate()).append("'  AND `HourNum` = ").append(obj.getHourNum()).toString();
													
								}
								else {
									query = queryBuilder(sb,obj, true).toString();
								}
								//System.out.println(query);							
								
								delPair.getValue().getReceivedSmsCount().addAndGet(-receivedCount);
								delPair.getValue().getReceivedSmsCredit().addAndGet(-receivedCredit);
								delPair.getValue().getSentSmsCount().addAndGet(-sentCount);
								delPair.getValue().getSentSmsCredit().addAndGet(-sentCredit);
								delPair.getValue().getCancelledSmsCount().addAndGet(-cancelledCount);
								delPair.getValue().getCancelledSmsCredit().addAndGet(-cancelledCredit);
								delPair.getValue().getDeliveredSmsCount().addAndGet(-deliveredCount);
								delPair.getValue().getDeliveredSmsCredit().addAndGet(-deliveredCredit);
								delPair.getValue().getLatencyLevel1().addAndGet(-obj.getLatencyLevel1().get());
								delPair.getValue().getLatencyLevel2().addAndGet(-obj.getLatencyLevel2().get());
								delPair.getValue().getLatencyLevel3().addAndGet(-obj.getLatencyLevel3().get());
								delPair.getValue().getLatencyLevel4().addAndGet(-obj.getLatencyLevel4().get());
								delPair.getValue().getFailedSmsCount().addAndGet(-failedCount);
								delPair.getValue().getFailedSmsCredit().addAndGet(-failedCredit);
								delPair.getValue().getFailedLatencyLevel1().addAndGet(-obj.getFailedLatencyLevel1().get());
								delPair.getValue().getFailedLatencyLevel2().addAndGet(-obj.getFailedLatencyLevel2().get() );
								delPair.getValue().getFailedLatencyLevel3().addAndGet(-obj.getFailedLatencyLevel3().get());
								delPair.getValue().getFailedLatencyLevel4().addAndGet(-obj.getFailedLatencyLevel4().get());								
								if (n >= Constants.dbConNum)n = 0;
								InsertIntoStatsMaster(query, 0, n);
								sb.setLength(0);
								query =null;
								processCount.getAndIncrement();
								n++;
								srNo=0;
							}
							
							
															
							
							}
													
						} 
					}
				  catch (Exception e) {
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
						//isRunning = false;
						serviceStatus = true;						
					}else {
						isRunning = true;
						break;
					}
				}
				
			}

		}
		System.out.println("  data inserted. Completed");
	}
	
	public void InsertIntoStatsMaster(final String queryStr, final int loopcount, final int x) {

		Statement stmt = null;
		int count = 0;
		boolean status = true;
		try {
			count = loopcount;
			//System.out.println(queryStr);
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
				InsertIntoStatsMaster(queryStr, ++count, x);
			} catch (InterruptedException e) {
				processCount.getAndDecrement();
				e.printStackTrace();
			}
		} catch (SQLTimeoutException sto) {
			System.out.println("Step -4");
			// sto.printStackTrace();
			try {
				Thread.sleep(1000);
				InsertIntoStatsMaster(queryStr, ++count, x);
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
					InsertIntoStatsMaster(queryStr, ++count, x);
				} else {
					Thread.sleep(1000);
					InsertIntoStatsMaster(queryStr, ++count, x);
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
					InsertIntoStatsMaster(queryStr, ++count, x);
				} else {
					Thread.sleep(1000);
					InsertIntoStatsMaster(queryStr, ++count, x);
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
			msql.printStackTrace();
			if (msql.getMessage().contains("smppstatsmaster") || msql.getMessage().contains("doesn't exist")) {
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
						InsertIntoStatsMaster(queryStr, ++count, x);
					} else {
						Thread.sleep(1000);
						InsertIntoStatsMaster(queryStr, ++count, x);
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
					InsertIntoStatsMaster(queryStr, ++count, x);
				} else {
					Thread.sleep(1000);
					InsertIntoStatsMaster(queryStr, ++count, x);
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
