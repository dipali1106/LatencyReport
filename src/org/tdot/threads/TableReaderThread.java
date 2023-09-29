package org.tdot.threads;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.tdot.connection.DbConnection;
import org.tdot.entity.ShnPromoTransSmppSmsDetails;
import org.tdot.entity.SmppStatsmaster;
import org.tdot.utils.Constants;

public class TableReaderThread extends Thread {
	public boolean serviceStatus = false;
	public boolean isRunning = true;
	int n = 0;
	String selectquery = "";
	ResultSet rs = null;
	Statement stmt = null;
	ResultSet rs2 = null;
	Statement stmt2 = null;
	String PhoneNumber = "";
	int SrNo = 0;
	int maxSrNo = 0;
	public static int maxLimit = 0;
	String Key = "";
	SmppStatsmaster smppStatsmasterDao;

	public void run() {
		maxLimit = Integer.parseInt(Constants.mapConInfo.get("MaxReadCount"));
		System.out.println("TableReaderThread thread started");
		while (isRunning) {
			if (Constants.tableBucket.size() > 0) {
				try {
					for (Entry<String, String> pair : Constants.tableBucket.entrySet()) {
						if (n >= Constants.dbConNum) {
							n = 0;
						}
						maxSrNo = 0;
						SrNo = 0;
						if (serviceStatus) {
							isRunning = false;
							break;
						}
						///System.out.print(pair.getValue());
						String CountQuery = "SELECT MessageSrNo FROM `" + pair.getValue()
								+ "` WHERE 1 ORDER By MessageSrNo DESC LIMIT 1";
						stmt2 = Constants.dataConnection.get(n).createStatement();
						rs2 = stmt2.executeQuery(CountQuery);
						// System.out.println(CountQuery);
						while (rs2.next()) {
							maxSrNo = rs2.getInt("MessageSrNo");
						}
						while (maxSrNo > SrNo) {
							// System.out.println(maxSrNo+", "+SrNo);
							if (serviceStatus) {
								break;
							}
							if (Constants.TotalBufferedRecord.get() <= 100) {
								selectquery = "SELECT `MessageSrNo`, `SenderId`,`Status`, `DlrSubmitDate`, `DlrDoneDate`, `ReportCode`,`MobileOperator`, `MobileOperatorCircle` ,`HourNum` FROM `"
										+ pair.getValue() + "` WHERE  `MessageSrNo` > " + SrNo + " LIMIT " + maxLimit;
								// System.out.println(selectquery);
								stmt = Constants.dataConnection.get(n).createStatement();
								rs = stmt.executeQuery(selectquery);
								while (rs.next()) {
									if (serviceStatus) {
										// break;
									}
									SrNo = rs.getInt("MessageSrNo");
									// System.out.println(pair.getKey());
									int UserId = Constants.userMasterDetail.get(rs.getString("SenderId"));
									int status = rs.getInt("Status");
									int reportcode = rs.getInt("ReportCode");
									String submitdate = rs.getString("DlrSubmitDate");
									String donedate = rs.getString("DlrDoneDate");
									Key = UserId + "##" + pair.getKey() + "##" + rs.getString("MobileOperator") + "##"
											+ rs.getString("MobileOperatorCircle") + "##" + rs.getInt("HourNum");

									if (Constants.statsDataBucket.get(Key) == null) {
										System.out.println(Key);
										smppStatsmasterDao = new SmppStatsmaster();
										smppStatsmasterDao.setUserId(UserId);
										smppStatsmasterDao.setOperatorName(rs.getString("MobileOperator"));
										smppStatsmasterDao.setCircleName(rs.getString("MobileOperatorCircle"));
										smppStatsmasterDao.setProcessDate(pair.getKey());
										smppStatsmasterDao.setHourNum(rs.getInt("HourNum"));
										Constants.statsDataBucket.put(Key, smppStatsmasterDao);
										Constants.TotalBufferedRecord.incrementAndGet();

									} else {
										smppStatsmasterDao = Constants.statsDataBucket.get(Key);
									}
									String dayDtl[] = pair.getKey().split("-");
									if (dayDtl.length == 3) {
										smppStatsmasterDao.setDayNum(Integer.parseInt(dayDtl[2]));
										smppStatsmasterDao.setMonthNum(Integer.parseInt(dayDtl[1]));
										smppStatsmasterDao.setYearNum(Integer.parseInt(dayDtl[0]));
									}
									smppStatsmasterDao.setReceivedSmsCount(smppStatsmasterDao.getReceivedSmsCount().incrementAndGet());
									smppStatsmasterDao.setReceivedSmsCredit(
											smppStatsmasterDao.getReceivedSmsCredit().incrementAndGet());
									if (status == 1) {
										smppStatsmasterDao.setSentSmsCount(
												smppStatsmasterDao.getSentSmsCount().incrementAndGet());
										smppStatsmasterDao.setSentSmsCredit(
												smppStatsmasterDao.getSentSmsCredit().incrementAndGet());
									} else if (status == 4) {
										smppStatsmasterDao.setCancelledSmsCount(
												smppStatsmasterDao.getCancelledSmsCount().incrementAndGet());
										smppStatsmasterDao.setCancelledSmsCredit(
												smppStatsmasterDao.getCancelledSmsCredit().incrementAndGet());
									}
									int latencySec = getDiffOfSubmitAndEndDate(submitdate, donedate);
									if (reportcode == 1) {
										smppStatsmasterDao.setDeliveredSmsCount(
												smppStatsmasterDao.getDeliveredSmsCount().incrementAndGet());
										smppStatsmasterDao.setDeliveredSmsCredit(
												smppStatsmasterDao.getDeliveredSmsCredit().incrementAndGet());
										if (latencySec < 15) {
											smppStatsmasterDao.setLatencyLevel1(
													smppStatsmasterDao.getLatencyLevel1().incrementAndGet());
										} else if (latencySec < 30) {
											smppStatsmasterDao.setLatencyLevel2(
													smppStatsmasterDao.getLatencyLevel2().incrementAndGet());
										} else if (latencySec < 60) {
											smppStatsmasterDao.setLatencyLevel3(
													smppStatsmasterDao.getLatencyLevel3().incrementAndGet());
										} else {
											smppStatsmasterDao.setLatencyLevel4(
													smppStatsmasterDao.getLatencyLevel4().incrementAndGet());
										}
									} else if (reportcode == 2) {
										smppStatsmasterDao.setFailedSmsCount(
												smppStatsmasterDao.getFailedSmsCount().incrementAndGet());
										smppStatsmasterDao.setFailedSmsCredit(
												smppStatsmasterDao.getFailedSmsCredit().incrementAndGet());
										if (latencySec < 15) {
											smppStatsmasterDao.setFailedLatencyLevel1(
													smppStatsmasterDao.getFailedLatencyLevel1().incrementAndGet());
										} else if (latencySec < 30) {
											smppStatsmasterDao.setFailedLatencyLevel2(
													smppStatsmasterDao.getFailedLatencyLevel2().incrementAndGet());
										} else if (latencySec < 60) {
											smppStatsmasterDao.setFailedLatencyLevel3(
													smppStatsmasterDao.getFailedLatencyLevel3().incrementAndGet());
										} else {
											smppStatsmasterDao.setFailedLatencyLevel4(
													smppStatsmasterDao.getFailedLatencyLevel4().incrementAndGet());
										}
									}

									Constants.statsDataBucket.put(Key, smppStatsmasterDao);

								}
							} else {
								Thread.sleep(500);
							}
							// Thread.sleep(2000);
							// System.out.println(SrNo);
						}

						Constants.tableBucket.remove(pair.getKey());
						Constants.statDates.add(pair.getKey());
					}
					Thread.sleep(500);
				} catch (SQLTimeoutException sto) {
					sto.printStackTrace();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} catch (SQLNonTransientConnectionException snt) {
					System.out.println("Step -8");
					snt.printStackTrace();
					try {
						DbConnection.getDataDbSqlConnection(n);

					} catch (Exception e) {
						e.printStackTrace();
					}
				} catch (SQLException se) {
					se.printStackTrace();
					Constants.consolelog
							.add("SQLException exception occured on position 1  In TableReaderThread.class run method: "
									+ se.getMessage());
					// TODO Auto-generated catch block
					// e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					Constants.consolelog.add(
							"InterruptedException exception occured on position 1  In TableReaderThread.class run method: "
									+ e.getMessage());

				} catch (Exception e) {
					Constants.consolelog
							.add("Exception exception occured on position 1  In TableReaderThread.class run method: "
									+ e.getMessage());
					e.printStackTrace();
				} finally {
					try {
						if (stmt2 != null)
							stmt2.close();
						if (stmt != null)
							stmt.close();
						if (rs != null)
							rs.close();
						if (rs2 != null)
							rs2.close();

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {
				// System.out.println("Here");
				isRunning = false;
			}
			// System.out.println("Total Buffered Count :"+
			// Constants.TotalBufferedRecord.get());

		}
		System.out.println("All Data Processed");
	}

	public int getDiffOfSubmitAndEndDate(String submitdate, String donedate) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1;
		try {
			if (submitdate != null && donedate != null) {
				d1 = sdf.parse(donedate);
				Date d2 = sdf.parse(submitdate);
				// Calculate time difference in milliseconds
				long diff_In_Time = d2.getTime() - d1.getTime();
				return (int) diff_In_Time;
			} else {
				return 0;
			}

		} catch (ParseException e) {
			e.printStackTrace();
			return 0;
		}
	}

}
