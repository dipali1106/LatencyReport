package org.tdot.utils;

import java.sql.Connection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.tdot.entity.SmppStatsmaster;

public class Constants {

	public static HashMap<String,String> mapConInfo=new HashMap<String,String>();
	public static ConcurrentHashMap<Integer, Connection>dataConnection=new ConcurrentHashMap<Integer, Connection>(1,0.9f,1);
	public static ConcurrentHashMap<Integer, Connection>masterConnection=new ConcurrentHashMap<Integer, Connection>(1,0.9f,1);
	public static ConcurrentLinkedQueue<String> consolelog=new ConcurrentLinkedQueue<String>();
	public static int dbConNum =10;
	public static String previousDate ="";
	public static String postfix="";
	public static String endDate="";
	public static long TotalPending =0;
	public static boolean keepRunning = false;
	public static AtomicInteger TotalBufferedRecord = new AtomicInteger(0);
	public static AtomicInteger TotalProcessedRecord = new AtomicInteger(0);
	public static String SmsTablePrefix ="shnpromotranssmppsmsdetails_";
	public static boolean isDataServerConnected= true;
	public static ConcurrentHashMap<String, Integer> userMasterDetail = new ConcurrentHashMap<String, Integer>();
	public static ConcurrentHashMap<String, String> tableBucket = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, SmppStatsmaster > statsDataBucket = new ConcurrentHashMap<String, SmppStatsmaster >();
	public static ConcurrentLinkedQueue<SmppStatsmaster> errorBucket = new ConcurrentLinkedQueue<SmppStatsmaster>();
	public static ConcurrentHashMap<String, ConcurrentLinkedQueue<String >> errorFailedNumbersBucket = new ConcurrentHashMap<String, ConcurrentLinkedQueue<String>>();
	public static ExecutorService executor;
	public static ConcurrentLinkedQueue<String> statDates = new ConcurrentLinkedQueue<String>();
	//public static ConcurrentHashMap<String, ServerConnectionProvider>cdrConnectionSet=new ConcurrentHashMap<String, ServerConnectionProvider>(1,0.9f,1);
	public static int TotalFailedCount = 0;
}
