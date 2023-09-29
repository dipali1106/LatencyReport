package org.tdot.entity;

public class ShnPromoTransSmppSmsDetails {
	private int MessageSrNo=0;
	private String SenderId="";
	private int Status=0;
	private String DlrSubmitDate="";
	private String DlrDoneDate="";
	private int ReportCode=0;
	private String ProcessDate="";
	private String MobileOperator="";
	public String getMobileOperator() {
		return MobileOperator;
	}
	public void setMobileOperator(String mobileOperator) {
		MobileOperator = mobileOperator;
	}
	private String MobileOperatorCircle ="";
	public String getMobileOperatorCircle() {
		return MobileOperatorCircle;
	}
	public void setMobileOperatorCircle(String mobileOperatorCircle) {
		MobileOperatorCircle = mobileOperatorCircle;
	}
	public int getMessageSrNo() {
		return MessageSrNo;
	}
	public void setMessageSrNo(int messageSrNo) {
		MessageSrNo = messageSrNo;
	}
	public String getSenderId() {
		return SenderId;
	}
	public void setSenderId(String senderId) {
		SenderId = senderId;
	}
	public int getStatus() {
		return Status;
	}
	public void setStatus(int status) {
		Status = status;
	}
	public String getDlrSubmitDate() {
		return DlrSubmitDate;
	}
	public void setDlrSubmitDate(String dlrSubmitDate) {
		DlrSubmitDate = dlrSubmitDate;
	}
	public String getDlrDoneDate() {
		return DlrDoneDate;
	}
	public void setDlrDoneDate(String dlrDoneDate) {
		DlrDoneDate = dlrDoneDate;
	}
	public int getReportCode() {
		return ReportCode;
	}
	public void setReportCode(int reportCode) {
		ReportCode = reportCode;
	}
	public String getProcessDate() {
		return ProcessDate;
	}
	public void setProcessDate(String processDate) {
		ProcessDate = processDate;
	}
	
	
	
}
