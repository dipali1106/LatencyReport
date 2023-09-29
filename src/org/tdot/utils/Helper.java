package org.tdot.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import org.tdot.connection.DbConnection;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

public class Helper {
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("d_MMM_yyyy");
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static Connection connect = null;

	public List<Date> getTablesInRange(Date StartDate, Date EndDate) {
		List<Date> dates = new ArrayList<Date>();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(StartDate);

		while (calendar.getTime().before(EndDate)) {
			Date result = calendar.getTime();			
			dates.add(result);
			calendar.add(Calendar.DATE, 1);
		}
		dates.add(calendar.getTime());

		return dates;
	}

	public String getTodayDate() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDateTime now = LocalDateTime.now();
		String today = dtf.format(now).toString();
		return today;
		// return "2_may_2023";
	}

	public long checkIfTableExist(List<Date> dates) {
		// String postfix = "";
		String tablename = null;
		Statement stmt = null;
		String query = null;
		ResultSet res = null;
		long totalRecord = 0;
		for (int i = 0; i < dates.size(); i++) {
			try {
				// postfix = dateFormatter.format(dates.get(i));
				tablename = Constants.SmsTablePrefix + dateFormatter.format(dates.get(i)).toLowerCase();
				query = "SELECT count(`MessageSrNo`) as c FROM `" + tablename + "`  WHERE 1 ";
				// System.out.println(query);
				while (Constants.dataConnection.get(0) == null || Constants.dataConnection.get(0).isClosed()) {
					DbConnection.getDataDbSqlConnection(0);
				}
				stmt = Constants.dataConnection.get(0).createStatement();
				res = stmt.executeQuery(query);
				while (res.next()) {
					if (res.getInt("c") != 0) {
						totalRecord += res.getInt("c");
						// System.out.println(totalRecord);
					}
				}
				//System.out.println(tablename);
				Constants.tableBucket.put(dateFormat.format(dates.get(i)), tablename);

			} catch (SQLNonTransientConnectionException se) {
				try {
					Thread.sleep(1000);
					DbConnection.getSqlConnection(0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} catch (CommunicationsException com) {
				try {
					Thread.sleep(1000);
					DbConnection.getSqlConnection(0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} catch (SQLSyntaxErrorException ssee) {
				if (ssee.getMessage().equalsIgnoreCase("Table 'tdot_websmpp." + tablename + "' doesn't exist")) {

				} else {
					ssee.printStackTrace();
				}

			} catch (SQLException sql) {
				if (sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed.")
						|| sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed")
						|| sql.getMessage()
								.equalsIgnoreCase("Could not retrieve transaction read-only status from server")) {
					try {
						Thread.sleep(1000);
						DbConnection.getSqlConnection(0);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				} else {
					Constants.consolelog.add(new StringBuilder()
							.append("Exception Position 1 Occured On Helper.class (checkIfTableExist Method) : ")
							.append(sql.getMessage()).toString());
				}
			} catch (NullPointerException np) {
				try {
					DbConnection.getSqlConnection(0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} catch (Exception e) {
				// e.printStackTrace();
				Constants.consolelog.add(new StringBuilder()
						.append("Exception Position 2 Occured On Helper.class (checkIfTableExist Method) : ")
						.append(e.getMessage()).toString());
			} finally {
				try {
					if (res != null)
						res.close();
					if (stmt != null)
						stmt.close();

				} catch (Exception ex) {
				}
			}
		}
		return totalRecord;
	}

	public String getPreviousDayDate() {
		String prevDate = "";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDateTime now = LocalDateTime.now();
		prevDate = dtf.format(now.minusDays(1)).toString();
		return prevDate;
	}

	public long getTotalPending() {
		long count = 0;
		Statement stmt = null;
		String query = null;
		ResultSet res = null;
		//connect = DbConnection.getConnection();
		for (Entry<String, String> pair : Constants.tableBucket.entrySet()) {
			try {
				// postfix = dateFormatter.format(dates.get(i));
				query = "SELECT count(`MessageSrNo`) as c FROM `" + pair.getValue() + "` WHERE  1 ";
				// System.out.println(query);
				while (Constants.dataConnection.get(0) == null || Constants.dataConnection.get(0).isClosed()) {
					DbConnection.getDataDbSqlConnection(0);
				}
				stmt = Constants.dataConnection.get(0).createStatement();
				res = stmt.executeQuery(query);
				while (res.next()) {
					if (res.getInt("c") != 0) {
						count += res.getInt("c");
						// System.out.println(res.getInt("c"));
					}
				}

			} catch (SQLNonTransientConnectionException se) {
				try {
					Thread.sleep(1000);
					DbConnection.getDataDbSqlConnection(0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} catch (CommunicationsException com) {
				try {
					Thread.sleep(1000);
					DbConnection.getDataDbSqlConnection(0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} catch (SQLSyntaxErrorException ssee) {
				if (ssee.getMessage().equalsIgnoreCase("Table 'tdot_websmpp." + pair.getValue() + "' doesn't exist")) {

				} else {
					ssee.printStackTrace();
				}

			} catch (SQLException sql) {
				if (sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed.")
						|| sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed")
						|| sql.getMessage()
								.equalsIgnoreCase("Could not retrieve transaction read-only status from server")) {
					try {
						Thread.sleep(1000);
						DbConnection.getDataDbSqlConnection(0);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				} else {
					Constants.consolelog.add(new StringBuilder()
							.append("Exception Position 1 Occured On Helper.class (checkIfTableExist Method) : ")
							.append(sql.getMessage()).toString());
				}
			} catch (NullPointerException np) {
				try {
					DbConnection.getDataDbSqlConnection(0);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} catch (Exception e) {
				// e.printStackTrace();
				Constants.consolelog.add(new StringBuilder()
						.append("Exception Position 2 Occured On Helper.class (checkIfTableExist Method) : ")
						.append(e.getMessage()).toString());
			} finally {
				try {
					if (res != null)
						res.close();
					if (stmt != null)
						stmt.close();
					if(connect!=null)connect.close();
				} catch (Exception ex) {
				}

			}
		}
		System.out.println(count);
		return count;
	}
	
	public void getUserMasterMap() {
		try {
			int n=0;
			List<String>  callTypeList=null;
			while(Constants.masterConnection.get(n) == null || Constants.masterConnection.get(n).isClosed()) {
				DbConnection.getSqlConnection(n);
			}
			String query="Select `SrNo`, `SenderId`, `UserId` from sms_sendermaster_dlt ";
			PreparedStatement ps = Constants.masterConnection.get(n).prepareStatement(query);
			ResultSet result = ps.executeQuery();
		    while(result.next())
			{			    	
		    	 int ProfileId = result.getInt("UserId");
				 Constants.userMasterDetail.put(result.getString("SenderId"), ProfileId);
			}
		  
		    ps.close();
		    result.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			Constants.consolelog.add("SQLException occured in Helper.java  getUserMasterMap method"+e1.getMessage());
			e1.printStackTrace();
		}
		catch(Exception e) {
			Constants.consolelog.add("Exception occured in Helper.java  getUserMasterMap method"+e.getMessage());

		}
		
	}
	
	public boolean isDataAlreadyProcessed(List<Date> dates) {
		boolean status =false;
		Statement stmt = null;
		ResultSet res = null;
		StringBuilder sb = new StringBuilder()
				.append("SELECT  COUNT(SrNo) as c FROM `smpp_statsmaster` WHERE `ProcessDate` IN (");// '2023-05-12')
		
		for (int i = 0; i < dates.size(); i++) {
			if (i == dates.size()-1) {
				sb.append("'").append(dateFormat.format(dates.get(i))).append("')");
			} else {
				sb.append("'").append(dateFormat.format(dates.get(i))).append("',");
			}

		}
//		/System.out.println(sb.toString());
		try {
			while (Constants.masterConnection.get(0) == null || Constants.masterConnection.get(0).isClosed()) {
				DbConnection.getSqlConnection(0);
			}
			
			stmt = Constants.dataConnection.get(0).createStatement();
			res = stmt.executeQuery(sb.toString());
			while (res.next()) {
				if (res.getInt("c") != 0) {
					status = true;
				}
			}
			res.close();
			
		} catch (SQLNonTransientConnectionException se) {
			try {
				Thread.sleep(1000);
				DbConnection.getSqlConnection(0);
				Constants.consolelog.add(new StringBuilder()
						.append("SQLNonTransientConnectionException Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(se.getMessage()).toString());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (CommunicationsException com) {
			try {
				Thread.sleep(1000);
				DbConnection.getSqlConnection(0);
				Constants.consolelog.add(new StringBuilder()
						.append("CommunicationsException Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(com.getMessage()).toString());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (SQLSyntaxErrorException ssee) {			
			ssee.printStackTrace();			
		} catch (SQLException sql) {
			
				Constants.consolelog.add(new StringBuilder()
						.append("Exception Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(sql.getMessage()).toString());
			
		} catch (NullPointerException np) {
			try {
				DbConnection.getSqlConnection(0);
				Constants.consolelog.add(new StringBuilder()
						.append("NullPointerException Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(np.getMessage()).toString());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			// e.printStackTrace();
			Constants.consolelog.add(new StringBuilder()
					.append("Exception Position 2 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
					.append(e.getMessage()).toString());
		} finally {
			try {
				if (res != null)res.close();
				if (stmt != null)stmt.close();

			} catch (Exception ex) {
			}
		}		
		return status;
	}

	public boolean isFailedNoProcessed(String monthDate) {
		SimpleDateFormat monthDtFormat = new SimpleDateFormat("MMM-yyyy");
		SimpleDateFormat monthFormat = new SimpleDateFormat("MM");
		SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
		boolean status =false;
		Statement stmt = null;
		ResultSet res = null;
		String month = "";
		String year="";
		try {
			 month = monthFormat.format(monthDtFormat.parse(monthDate));
			 year = yearFormat.format(monthDtFormat.parse(monthDate));
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String failedNoQuery = "Select `SrNo` from `failedmobilemaster` WHERE  MONTH(`ProcessDate`) ='" + month+ "' AND YEAR(`ProcessDate`) ='" + year + "' LIMIT 1";	
		//System.out.println(failedNoQuery);
		try {
			while (Constants.masterConnection.get(0) == null || Constants.masterConnection.get(0).isClosed()) {
				DbConnection.getSqlConnection(0);
			}
			
			stmt = Constants.dataConnection.get(0).createStatement();
			res = stmt.executeQuery(failedNoQuery);
			while (res.next()) {
				//System.out.println(res.getInt("SrNo"));
				if (res.getInt("SrNo") != 0) {
					status = true;
				}
			}			
		} catch (SQLNonTransientConnectionException se) {
			try {
				Thread.sleep(1000);
				DbConnection.getSqlConnection(0);
				Constants.consolelog.add(new StringBuilder()
						.append("SQLNonTransientConnectionException Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(se.getMessage()).toString());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (CommunicationsException com) {
			try {
				Thread.sleep(1000);
				DbConnection.getSqlConnection(0);
				Constants.consolelog.add(new StringBuilder()
						.append("CommunicationsException Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(com.getMessage()).toString());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (SQLSyntaxErrorException ssee) {			
			ssee.printStackTrace();			
		} catch (SQLException sql) {
			
				Constants.consolelog.add(new StringBuilder()
						.append("Exception Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(sql.getMessage()).toString());
			
		} catch (NullPointerException np) {
			try {
				DbConnection.getSqlConnection(0);
				Constants.consolelog.add(new StringBuilder()
						.append("NullPointerException Position 1 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
						.append(np.getMessage()).toString());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			// e.printStackTrace();
			Constants.consolelog.add(new StringBuilder()
					.append("Exception Position 2 Occured On Helper.class (isDataAlreadyProcessed Method) : ")
					.append(e.getMessage()).toString());
		} finally {
			try {
				if (res != null)res.close();
				if (stmt != null)stmt.close();

			} catch (Exception ex) {
			}
		}
			
		return status;
	}

}
