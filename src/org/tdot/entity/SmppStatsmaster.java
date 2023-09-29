package org.tdot.entity;

import java.util.concurrent.atomic.AtomicInteger;

public class SmppStatsmaster {
	private int SrNo=0;
	private int UserId;
	private String OperatorName;
	private String CircleName;
	private String ProcessDate;
	private int HourNum;
	private int DayNum;
	private int MonthNum;
	private int YearNum;
	private AtomicInteger ReceivedSmsCount = new AtomicInteger(0) ;	
	private AtomicInteger ReceivedSmsCredit = new AtomicInteger(0) ;
	private AtomicInteger SentSmsCount = new AtomicInteger(0);
	private AtomicInteger SentSmsCredit =new AtomicInteger(0);
	private AtomicInteger DeliveredSmsCount = new AtomicInteger(0);
	private AtomicInteger DeliveredSmsCredit = new AtomicInteger(0);
	private AtomicInteger FailedSmsCount = new AtomicInteger(0);
	private AtomicInteger FailedSmsCredit =new AtomicInteger(0);
	private AtomicInteger LatencyLevel1 = new AtomicInteger(0);
	private AtomicInteger LatencyLevel2 = new AtomicInteger(0);
	private AtomicInteger LatencyLevel3 = new AtomicInteger(0);
	private AtomicInteger LatencyLevel4 = new AtomicInteger(0);
	private int Active=1;
	//private String RequestSource;
	//private String EntryDateTime;
	private AtomicInteger CancelledSmsCount = new AtomicInteger(0);
	private AtomicInteger CancelledSmsCredit =  new AtomicInteger(0);
	private AtomicInteger FailedLatencyLevel1 =  new AtomicInteger(0);
	private AtomicInteger FailedLatencyLevel2 =  new AtomicInteger(0);
	private AtomicInteger FailedLatencyLevel3 =  new AtomicInteger(0);
	private AtomicInteger FailedLatencyLevel4=  new AtomicInteger(0);
	
//	public SmppStatsmaster() {
//		this.UserId =0;
//		this.ProcessDate="";
//		this.HourNum=0;
//		this.DayNum=0;
//		this.MonthNum=0;
//		this.YearNum=0;
//		this.ReceivedSmsCount=0;
//		this.ReceivedSmsCredit.set(0);
//		this.SentSmsCount=0;
//		this.SentSmsCredit=0;
//		this.DeliveredSmsCount=0;
//		this.DeliveredSmsCredit=0;
//		this.FailedSmsCount=0;
//		this.FailedSmsCredit=0;
//		this.LatencyLevel1=0;
//		this.LatencyLevel2=0;
//		this.LatencyLevel3=0;
//		this.LatencyLevel4=0;
//		this.Active=1;
//		this.FailedLatencyLevel1=0;
//		this.FailedLatencyLevel2=0;
//		this.FailedLatencyLevel3=0;
//		this.FailedLatencyLevel4=0;
//	}
	public SmppStatsmaster() {
		
	}
	
	public SmppStatsmaster(SmppStatsmaster value) {
		this.UserId =value.getUserId();
		this.OperatorName = value.getOperatorName();
		this.CircleName = value.getCircleName();
		this.ProcessDate=value.getProcessDate();
		this.HourNum= value.getHourNum();
		this.DayNum=value.getDayNum();
		this.MonthNum=value.getMonthNum();
		this.YearNum=value.getYearNum();
		this.ReceivedSmsCount=value.getReceivedSmsCount();
		this.ReceivedSmsCredit= value.getReceivedSmsCredit();
		this.SentSmsCount=value.getSentSmsCount();
		this.SentSmsCredit=value.getSentSmsCredit();
		this.DeliveredSmsCount=value.getDeliveredSmsCount();
		this.DeliveredSmsCredit=value.getDeliveredSmsCredit();
		this.FailedSmsCount=value.getFailedSmsCount();
		this.FailedSmsCredit=value.getFailedSmsCredit();
		this.LatencyLevel1=value.getLatencyLevel1();
		this.LatencyLevel2=value.getLatencyLevel2();
		this.LatencyLevel3=value.getLatencyLevel3();
		this.LatencyLevel4=value.getLatencyLevel4();
		this.FailedLatencyLevel1=value.getFailedLatencyLevel1();
		this.FailedLatencyLevel2=value.getFailedLatencyLevel2();
		this.FailedLatencyLevel3=value.getFailedLatencyLevel3();
		this.FailedLatencyLevel4=value.getFailedLatencyLevel4();
				
	}
	public int getSrNo() {
		return SrNo;
	}
	public void setSrNo(int srNo) {
		SrNo = srNo;
	}
	public int getUserId() {
		return UserId;
	}
	public void setUserId(int userId) {
		UserId = userId;
	}
	public String getOperatorName() {
		return OperatorName;
	}
	public void setOperatorName(String operatorName) {
		OperatorName = operatorName;
	}
	public String getCircleName() {
		return CircleName;
	}
	public void setCircleName(String circleName) {
		CircleName = circleName;
	}
	public String getProcessDate() {
		return ProcessDate;
	}
	public void setProcessDate(String processDate) {
		ProcessDate = processDate;
	}
	public int getHourNum() {
		return HourNum;
	}
	public void setHourNum(int hourNum) {
		HourNum = hourNum;
	}
	public int getDayNum() {
		return DayNum;
	}
	public void setDayNum(int dayNum) {
		DayNum = dayNum;
	}
	public int getMonthNum() {
		return MonthNum;
	}
	public void setMonthNum(int monthNum) {
		MonthNum = monthNum;
	}
	public int getYearNum() {
		return YearNum;
	}
	public void setYearNum(int yearNum) {
		YearNum = yearNum;
	}
	public AtomicInteger getReceivedSmsCount() {
		return ReceivedSmsCount;
	}
	public void setReceivedSmsCount(int receivedSmsCount) {
		ReceivedSmsCount.set(receivedSmsCount);
	}
	public AtomicInteger getReceivedSmsCredit() {
		return ReceivedSmsCredit;
	}
	public void setReceivedSmsCredit(int receivedSmsCredit) {
		ReceivedSmsCredit.set(receivedSmsCredit);
	}
	public AtomicInteger getSentSmsCount() {
		return SentSmsCount;
	}
	public void setSentSmsCount(int sentSmsCount) {
		SentSmsCount.set(sentSmsCount);
	}
	public AtomicInteger getSentSmsCredit() {
		return SentSmsCredit;
	}
	public void setSentSmsCredit(int sentSmsCredit) {
		SentSmsCredit.set(sentSmsCredit);
	}
	public AtomicInteger getDeliveredSmsCount() {
		return DeliveredSmsCount;
	}
	public void setDeliveredSmsCount(int deliveredSmsCount) {
		DeliveredSmsCount.set(deliveredSmsCount);
	}
	public AtomicInteger getDeliveredSmsCredit() {
		return DeliveredSmsCredit;
	}
	public void setDeliveredSmsCredit(int deliveredSmsCredit) {
		DeliveredSmsCredit.set(deliveredSmsCredit);
	}
	public AtomicInteger getFailedSmsCount() {
		return FailedSmsCount;
	}
	public void setFailedSmsCount(int failedSmsCount) {
		FailedSmsCount.set(failedSmsCount);
	}
	public AtomicInteger getFailedSmsCredit() {
		return FailedSmsCredit;
	}
	public void setFailedSmsCredit(int failedSmsCredit) {
		FailedSmsCredit.set(failedSmsCredit);
	}
	public AtomicInteger getLatencyLevel1() {
		return LatencyLevel1;
	}
	public void setLatencyLevel1(int latencyLevel1) {
		LatencyLevel1.set(latencyLevel1);
	}
	public AtomicInteger getLatencyLevel2() {
		return LatencyLevel2;
	}
	public void setLatencyLevel2(int latencyLevel2) {
		LatencyLevel2.set(latencyLevel2);
	}
	public AtomicInteger getLatencyLevel3() {
		return LatencyLevel3;
	}
	public void setLatencyLevel3(int latencyLevel3) {
		LatencyLevel3.set(latencyLevel3);
	}
	public AtomicInteger getLatencyLevel4() {
		return LatencyLevel4;
	}
	public void setLatencyLevel4(int latencyLevel4) {
		LatencyLevel4.set(latencyLevel4);
	}
	public int getActive() {
		return Active;
	}
	public void setActive(int active) {
		Active = active;
	}	
	
	public AtomicInteger getCancelledSmsCount() {
		return CancelledSmsCount;
	}
	public void setCancelledSmsCount(int cancelledSmsCount) {
		CancelledSmsCount.set(cancelledSmsCount);
	}
	public AtomicInteger getCancelledSmsCredit() {
		return CancelledSmsCredit;
	}
	public void setCancelledSmsCredit(int cancelledSmsCredit) {
		CancelledSmsCredit.set(cancelledSmsCredit);
	}
	public AtomicInteger getFailedLatencyLevel1() {
		return FailedLatencyLevel1;
	}
	public void setFailedLatencyLevel1(int failedLatencyLevel1) {
		FailedLatencyLevel1.set(failedLatencyLevel1);
	}
	public AtomicInteger getFailedLatencyLevel2() {
		return FailedLatencyLevel2;
	}
	public void setFailedLatencyLevel2(int failedLatencyLevel2) {
		FailedLatencyLevel2.set(failedLatencyLevel2);
	}
	public AtomicInteger getFailedLatencyLevel3() {
		return FailedLatencyLevel3;
	}
	public void setFailedLatencyLevel3(int failedLatencyLevel3) {
		FailedLatencyLevel3.set(failedLatencyLevel3);
	}
	public AtomicInteger getFailedLatencyLevel4() {
		return FailedLatencyLevel4;
	}
	public void setFailedLatencyLevel4(int failedLatencyLevel4) {
		FailedLatencyLevel4.set(failedLatencyLevel4);
	}
	
	
}
