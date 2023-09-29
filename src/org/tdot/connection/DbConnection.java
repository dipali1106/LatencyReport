package org.tdot.connection;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.List;

import org.tdot.utils.Constants;

import com.mysql.cj.jdbc.exceptions.CommunicationsException;


public class DbConnection {
	 private static Connection connect=null;
	public static DbConnection dbCon =null;
	public static DbConnection getInstance() {
		if(dbCon== null) {
			dbCon = new DbConnection();
		}
		return dbCon;
		
	}
	
	public static void readDbConfigFile() {
		 try {
			String path = new StringBuilder(new File("").getCanonicalPath()).append(File.separator).append("dbconfig").append(File.separator).append("dbconfiginfo").toString();
			Path p = Paths.get(new File(path).getAbsolutePath());
			List<String> arr=Files.readAllLines(p);
			for(int i=0;i<arr.size();i++) {
				 if( arr.get(i)!=null &&  arr.get(i).trim().length()>0) {
					 String str[] = arr.get(i).split("=");	
					   if(str.length==2) {
						   Constants.mapConInfo.put(str[0],str[1]);
					   }
				 }				 
			 }
				
		 } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static Connection getConnection() 
    {   
		try {
		 if(connect!=null) {
				connect.close();
			}        
			Class.forName("com.mysql.cj.jdbc.Driver");		
			connect= DriverManager.getConnection("jdbc:mysql://"+Constants.mapConInfo.get("MasterHost")+":"+Constants.mapConInfo.get("MasterPort")+"/"+Constants.mapConInfo.get("MasterDb")+"?useSSL=false&useUnicode=yes&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&rewriteBatchedStatement=true",Constants.mapConInfo.get("MasterUser"),Constants.mapConInfo.get("MasterPassword"));
		} catch (SQLException e) {
			e.printStackTrace();
			Constants.consolelog.add(new StringBuilder().append("Check Connection Details in File.  SQLException occured in DbConnection.class -( getConnection method) :").append(e.getMessage()).toString());
		//	WebSmsIndex.textAreaLog.append("Check Connection Details, Error occured:"+e.getMessage()+"\n");
		} catch (ClassNotFoundException e) {
			//WebSmsIndex.textAreaLog.append("Contact Administrator, Error occured:"+e.getMessage()+"\n");
			Constants.consolelog.add(new StringBuilder().append("ClassNotFoundException occured in DbConnection.class -( getConnection method) :").append(e.getMessage()).toString());
		}
       return connect;  
          
    }
	public boolean createRemoteConnection() {		
		boolean status = false;
		String strHost = Constants.mapConInfo.get("DataHost");
		String dbName = Constants.mapConInfo.get("DataDb");
		String user = Constants.mapConInfo.get("DataUser");
		String password = Constants.mapConInfo.get("DataPassword");
		
		String MasterHost = Constants.mapConInfo.get("MasterHost");
		String MasterDb = Constants.mapConInfo.get("MasterDb");
		String Masteruser = Constants.mapConInfo.get("MasterUser");
		String Masterpassword = Constants.mapConInfo.get("MasterPassword");
		//String tableName = tableName;
		try {
			while (connect == null || connect.isClosed()) {
				connect = getConnection();
				Thread.sleep(100);
			}
			
			for (int i = 0; i < Constants.dbConNum; i++) {
				Constants.dataConnection.put(i, DriverManager.getConnection(new StringBuilder().append("jdbc:mysql://")
						.append(strHost).append(":3306/").append(dbName)
						.append("?useUnicode=yes&characterEncoding=UTF-8&useSSL=false&connectTimeout=5000&socketTimeout=5000&user=")
						.append(user).append("&password=").append(password).toString()));
				//Constants.isDataServerConnected = true;
			}
			for (int i = 0; i < Constants.dbConNum; i++) {
				Constants.masterConnection.put(i, DriverManager.getConnection(new StringBuilder().append("jdbc:mysql://")
						.append(MasterHost).append(":3306/").append(MasterDb)
						.append("?useUnicode=yes&characterEncoding=UTF-8&useSSL=false&connectTimeout=5000&socketTimeout=5000&user=")
						.append(Masteruser).append("&password=").append(Masterpassword).toString()));
				//Constants.isDataServerConnected = true;
			}
			
		}catch (CommunicationsException com) {
			try {
				Thread.sleep(1000);
				getConnection();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			Constants.isDataServerConnected = false;
		} catch (SQLNonTransientConnectionException se) {
			try {
				Thread.sleep(1000);
				getConnection();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			Constants.isDataServerConnected = false;
		}  catch (SQLException sql) {
			if (sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed.")
					|| sql.getMessage().equalsIgnoreCase("No operations allowed after statement closed")
					|| sql.getMessage()
							.equalsIgnoreCase("Could not retrieve transaction read-only status from server")) {
				try {
					Thread.sleep(1000);
					getConnection();
				} catch (Exception e1) {
					//WebSmsIndex.textAreaLog.append("Please check connection details in server_master; SQLException occured while connecting  :"
						//			+ e1.getMessage() + "\n");
					 e1.printStackTrace();
				}
				Constants.isDataServerConnected = false;
			} else {
				Constants.isDataServerConnected = false;
				
				Constants.consolelog.add(new StringBuilder().append(
						"Exception Position 1 Occured On DbConnection.class (createRemoteConnection Method) : ")
						.append(sql.getMessage()).toString());
			}
		} catch (NullPointerException np) {
			try {
				Thread.sleep(1000);
				getConnection();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			Constants.isDataServerConnected = false;
		} catch (Exception e) {
			// e.printStackTrace();
			Constants.isDataServerConnected = false;
			Constants.consolelog.add(new StringBuilder()
					.append("Exception Position 2 Occured On DbConnection.class (createRemoteConnection Method) : ")
					.append(e.getMessage()).toString());
		} finally {
			
		}
		return status;
	}
	
	
	public static synchronized boolean getSqlConnection(int x) {
		Statement smt=null;
		boolean isAlive=true;
		boolean status=false;
		try {
			if(Constants.masterConnection.get(x)==null){
				readDbConfigFile();
				Class.forName("com.mysql.cj.jdbc.Driver");
				Constants.masterConnection.put(x, DriverManager.getConnection(new StringBuilder().append("jdbc:mysql://").append(Constants.mapConInfo.get("MasterHost")).append(":").append(Constants.mapConInfo.get("MasterPort")).append("/").append(Constants.mapConInfo.get("MasterDb")).append("?useSSL=false&user=").append(Constants.mapConInfo.get("MasterUser")).append("&password=").append(Constants.mapConInfo.get("MasterPassword")).toString()));
				status=true;
			}
			else if(Constants.masterConnection.get(x)!=null){
				try{
					smt = Constants.masterConnection.get(x).createStatement();
					smt.close();
					status=true;
				}
				catch(Exception e1){
					isAlive=false;
				}
				if(!isAlive){
					Constants.masterConnection.get(x).close();
					Class.forName("com.mysql.jdbc.Driver");
					Constants.masterConnection.put(x, DriverManager.getConnection(new StringBuilder().append("jdbc:mysql://").append(Constants.mapConInfo.get("MasterHost")).append(":").append(Constants.mapConInfo.get("MasterPort")).append("/").append(Constants.mapConInfo.get("MasterDb")).append("?useSSL=false&user=").append(Constants.mapConInfo.get("MasterUser")).append("&password=").append(Constants.mapConInfo.get("MasterPassword")).toString()));
					status=true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			try{
				if(smt!=null) smt.close();
			}
			catch(Exception ex){}
		}
		return status;				
	}
	
	
	public static synchronized boolean getDataDbSqlConnection(int x) {
		Statement smt=null;
		boolean isAlive=true;
		boolean status=false;
		try {
			if(Constants.dataConnection.get(x)==null){
				readDbConfigFile();
				Class.forName("com.mysql.cj.jdbc.Driver");
				Constants.dataConnection.put(x, DriverManager.getConnection(new StringBuilder().append("jdbc:mysql://").append(Constants.mapConInfo.get("DataHost")).append(":").append(Constants.mapConInfo.get("DataPort")).append("/").append(Constants.mapConInfo.get("DataDb")).append("?useSSL=false&user=").append(Constants.mapConInfo.get("MasterUser")).append("&password=").append(Constants.mapConInfo.get("DataPassword")).toString()));
				status=true;
			}
			else if(Constants.dataConnection.get(x)!=null){
				try{
					smt = Constants.dataConnection.get(x).createStatement();
					smt.close();
					status=true;
				}
				catch(Exception e1){
					isAlive=false;
				}
				if(!isAlive){
					Constants.dataConnection.get(x).close();
					Class.forName("com.mysql.jdbc.Driver");
					Constants.dataConnection.put(x, DriverManager.getConnection(new StringBuilder().append("jdbc:mysql://").append(Constants.mapConInfo.get("DataHost")).append(":").append(Constants.mapConInfo.get("DataPort")).append("/").append(Constants.mapConInfo.get("DataDb")).append("?useSSL=false&user=").append(Constants.mapConInfo.get("MasterUser")).append("&password=").append(Constants.mapConInfo.get("DataPassword")).toString()));
					status=true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			try{
				if(smt!=null) smt.close();
			}
			catch(Exception ex){}
		}
		return status;				
	}
	
	
}
