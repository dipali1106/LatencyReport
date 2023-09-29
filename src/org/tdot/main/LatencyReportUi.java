package org.tdot.main;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JFormattedTextField.AbstractFormatter;
import javax.swing.border.EmptyBorder;

import org.jdatepicker.impl.JDatePanelImpl;
import org.jdatepicker.impl.JDatePickerImpl;
import org.jdatepicker.impl.UtilDateModel;
import org.tdot.connection.DbConnection;
import org.tdot.threads.LoggingThread;
import org.tdot.threads.StatsInsertUpdateThread;
import org.tdot.threads.StatsmasterInsertThread;
import org.tdot.threads.StatsmasterSingleInsert;
import org.tdot.threads.TableReaderThread;
import org.tdot.utils.Constants;
import org.tdot.utils.Helper;
import java.awt.Color;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.swing.JButton;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.ScrollPaneConstants;

public class LatencyReportUi extends JFrame {

	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	JLabel lblConnectionStatus = new JLabel("Connection Status");
	JLabel lblAppStatus = new JLabel("App Status");
	JLabel lblTotalFetched = new JLabel("Total Fetched");
	JLabel lblTotal = new JLabel("Total Record");
	JLabel conStatus = new JLabel("");
	JLabel appStatus = new JLabel("");
	JLabel totalPending = new JLabel("");
	JLabel totlaFetched = new JLabel("");
	JButton btnConnect = new JButton("Connect");
	JButton btnForceExit = new JButton("Force Exit");
	JButton btnExit = new JButton("Exit");
	JLabel lblStartDate = new JLabel("Start Date");
	JLabel lblEndDate = new JLabel("End Date");
	JButton btnStart = new JButton("start");
	JButton btnStop = new JButton("Stop");
	public static JTextArea textAreaLog = new JTextArea();
	UtilDateModel model1 = new UtilDateModel();
	UtilDateModel model2 = new UtilDateModel();
	JDatePanelImpl datePanel = null;
	JDatePickerImpl datePicker1 = null;
	JDatePanelImpl datePanel2 = null;
	JDatePickerImpl datePicker2 = null;
	private String datePattern = "yyyy-MM-dd";
	private SimpleDateFormat dateFormatter = new SimpleDateFormat(datePattern);
	Helper helper = new Helper();
	TableReaderThread tableReaderThread = new TableReaderThread();
	LoggingThread loggingThread = new LoggingThread();
	UiUpdate uiUpdate = new UiUpdate();
	StatsInsertUpdateThread statsInsertUpdateThread;
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					LatencyReportUi frame = new LatencyReportUi();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public LatencyReportUi() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 650, 600);
		setTitle("Sms Latency Report");
		contentPane = new JPanel();
		contentPane.setBackground(new Color(255, 255, 255));
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));

		setContentPane(contentPane);
		contentPane.setLayout(null);
		lblConnectionStatus.setBounds(110, 36, 150, 25);
		lblAppStatus.setBounds(113, 71, 138, 25);
		contentPane.add(lblAppStatus);
		contentPane.add(lblConnectionStatus);
		contentPane.add(conStatus);
		contentPane.add(lblTotal);
		contentPane.add(totalPending);
		contentPane.add(appStatus);
		contentPane.add(lblTotalFetched);
		contentPane.add(totlaFetched);
		contentPane.add(btnConnect);
		contentPane.add(btnExit);
		contentPane.add(btnForceExit);
		contentPane.add(lblStartDate);
		contentPane.add(lblEndDate);
		contentPane.add(btnStart);
		contentPane.add(btnStop);
		lblTotalFetched.setBounds(110, 141, 120, 25);		
		lblTotal.setBounds(110, 106, 120, 25);
		conStatus.setBounds(340, 36, 220, 25);
		appStatus.setFont(new Font("Dialog", Font.BOLD, 12));
		appStatus.setBounds(340, 71, 220, 25);
		totalPending.setBounds(340, 106, 140, 25);
		totlaFetched.setBounds(340, 141, 140, 25);		
		btnConnect.setBounds(113, 329, 100, 25);		
		btnExit.setForeground(Color.WHITE);
		btnExit.setBackground(new Color(204, 0, 0));
		btnExit.setBounds(253, 395, 80, 25);
		btnForceExit.setForeground(Color.WHITE);
		btnForceExit.setBackground(new Color(204, 0, 0));
		btnForceExit.setBounds(386, 395, 110, 25);		
		lblStartDate.setBounds(115, 202, 105, 25);
		lblEndDate.setBounds(115, 247, 80, 25);
		btnStart.setEnabled(false);
		btnStart.setBounds(253, 329, 80, 25);		
		btnStop.setBounds(386, 329, 80, 25);		
		model1.setValue(new Date());
		model1.setSelected(true);
		Properties p = new Properties();
		p.put("text.today", "Today");
		p.put("text.month", "Month");
		p.put("text.year", "Year");
		datePanel = new JDatePanelImpl(model1, p);
		datePicker1 = new JDatePickerImpl(datePanel, new DateLabelFormatter());
		datePicker1.setTextEditable(true);
		datePicker1.setShowYearButtons(true);
		datePicker1.setDoubleClickAction(true);
		datePicker1.setButtonFocusable(true);
		datePicker1.setSize(150, 25);
		datePicker1.setLocation(300, 202);
		model2.setValue(new Date());
		model2.setSelected(true);
		// model2.addYear(0);
		datePanel2 = new JDatePanelImpl(model2, p);
		datePicker2 = new JDatePickerImpl(datePanel2, new DateLabelFormatter2());
		datePicker2.setTextEditable(true);
		datePicker2.setShowYearButtons(true);
		datePicker2.setDoubleClickAction(true);
		datePicker2.setButtonFocusable(true);
		datePicker2.setSize(150, 25);
		datePicker2.setLocation(300, 247);
		contentPane.add(datePicker1);
		contentPane.add(datePicker2);		
		JScrollPane scrollLog = new JScrollPane();
		scrollLog.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		scrollLog.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scrollLog.setBounds(10, 432, 600, 125);
		contentPane.add(scrollLog);
		scrollLog.setViewportView(textAreaLog);
		scrollLog.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scrollLog.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textAreaLog.setBounds(55, 420, 755, 164);

		btnConnect.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				DbConnection.readDbConfigFile();
				Connection con = null;
				try {
					con = DbConnection.getConnection();
					if (con == null) {
						conStatus.setText("Not Connected");
						textAreaLog.setText("Check Connection file, Error in Master Db Connection.");
					} else if (!con.isClosed()) {
						con.close();
						DbConnection.getInstance().createRemoteConnection();
						if (Constants.isDataServerConnected == true) {
							
							conStatus.setOpaque(true);
							conStatus.setBackground(new Color(45, 221, 21));
							conStatus.setText("Connected");
							loggingThread =new  LoggingThread();
							loggingThread.start();
							btnStart.setEnabled(true);
						
							// btnStop.setEnabled(true);
							appStatus.setText(" Not started");
						} else {
							conStatus.setText("Not Connected,Issue in DataDb Connection");
						}
					} else {
						conStatus.setText("Not Connected, Issue in MasterDb Connection");
					}
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					textAreaLog.append(e1.getMessage());
					e1.printStackTrace();
				}
			}
		});

		
		btnStart.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				DbConnection.readDbConfigFile();
				Connection con = null;
				try {
					con = DbConnection.getConnection();
					if (con == null) {
						conStatus.setText("Not Connected");
						textAreaLog.setText("Check Connection file, Error in Master Db Connection.");
					} else if (!con.isClosed()) {
						con.close();
						DbConnection.getInstance().createRemoteConnection();
						if (Constants.isDataServerConnected == true) {
							conStatus.setOpaque(true);
							conStatus.setBackground(new Color(45, 221, 21));
							conStatus.setText("Connected");
							String strtDate = datePicker1.getJFormattedTextField().getText();
							String endDate = datePicker2.getJFormattedTextField().getText();
							System.out.println(strtDate + "  " + endDate);
						
							if (dateFormatter.parse(strtDate).compareTo(dateFormatter.parse(endDate)) > 0) {
								JOptionPane.showMessageDialog(datePicker1, "Start Date Can not be ahead of End Date");
							} else {						
								List<Date> dates = helper.getTablesInRange(dateFormatter.parse(strtDate),dateFormatter.parse(endDate));
								if(helper.isDataAlreadyProcessed(dates)) {
									JOptionPane.showMessageDialog(btnStart, "Data already processed for given range of date");
								}
								else {
									long totalRecord = helper.checkIfTableExist(dates);
									Constants.TotalPending = helper.getTotalPending();
									totalPending.setText(String.valueOf(Constants.TotalPending));
									if (Constants.tableBucket.size() > 0) {	
										int poolsize = Constants.mapConInfo.get("PoolSize")!=null? Integer.parseInt(Constants.mapConInfo.get("PoolSize")):10;
										Constants.executor =  Executors.newFixedThreadPool(poolsize);
										Constants.keepRunning=true;
										textAreaLog.setText("");
										btnStart.setEnabled(false);
										btnStop.setEnabled(true);
										appStatus.setText(" Running");
										appStatus.setOpaque(true);
										appStatus.setBackground(new Color(45, 221, 21));
										helper.getUserMasterMap();
										tableReaderThread = new TableReaderThread();
										tableReaderThread.isRunning = true;
										tableReaderThread.start();
										statsInsertUpdateThread = new StatsInsertUpdateThread();
										statsInsertUpdateThread.isRunning=true;
										statsInsertUpdateThread.start();
										/*
										statsmasterInsertThread = new StatsmasterInsertThread();
										statsmasterInsertThread.start();
										statsmasterSingleInsert = new StatsmasterSingleInsert();
										statsmasterSingleInsert.start();
										*/
										uiUpdate= new UiUpdate();
										uiUpdate.start();
									} else {
										JOptionPane.showMessageDialog(btnStart, "Table does not exist in given range of date");
									}
								}
							}
							
						}else {
							conStatus.setOpaque(false);
							conStatus.setText("Not Connected,Issue in DataDb Connection");
						}
					}
				} catch (ParseException e1) {
					e1.printStackTrace();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		});

		btnStop.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				// TODO Auto-generated method stub
				new Thread(new Runnable() {
					
					@Override
					public void run() {
						// TODO Auto-generated method stub
						appStatus.setText("Stopping.Please wait.");
						tableReaderThread.serviceStatus= true;						
						while(true) {
							Constants.keepRunning=false;
							if(tableReaderThread==null ) {break;}
							else if(!tableReaderThread.isAlive()) {break;}
							else {
								try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
						statsInsertUpdateThread.serviceStatus =true;
						while(true) {
							Constants.keepRunning=false;
							if(tableReaderThread==null && statsInsertUpdateThread==null  && uiUpdate==null) {
								break;
							}
							else if(!tableReaderThread.isAlive() && !statsInsertUpdateThread.isAlive() && !uiUpdate.isAlive()) {
								break;
							}
							else {
								System.out.println("tableReaderThread="+tableReaderThread.isAlive()+",statsInsertUpdateThread"+statsInsertUpdateThread.isAlive()+", uiUpdate="+uiUpdate.isAlive());
								try {
									Thread.sleep(3000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							
						}						
						appStatus.setText("Stopped.");
						appStatus.setOpaque(false);
						btnStart.setEnabled(true);
						btnStop.setEnabled(false);
					}
				}).start();
		
			}
		});
		
		
		btnExit.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				new Thread(new Runnable() {					
					@Override
					public void run() {
						appStatus.setText("Stopping.Please wait.");
						tableReaderThread.serviceStatus= true;						
						while(true) {
							Constants.keepRunning=false;
							if(tableReaderThread==null ) {break;}
							else if(!tableReaderThread.isAlive()) {break;}
							else {
								try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
						statsInsertUpdateThread.serviceStatus =true;
						while(true) {
							Constants.keepRunning=false;
							if(tableReaderThread==null && statsInsertUpdateThread==null  && uiUpdate==null) {
								break;
							}
							else if(!tableReaderThread.isAlive() && !statsInsertUpdateThread.isAlive() && !uiUpdate.isAlive()) {
								break;
							}
							else {
								System.out.println("tableReaderThread="+tableReaderThread.isAlive()+",statsInsertUpdateThread"+statsInsertUpdateThread.isAlive()+", uiUpdate="+uiUpdate.isAlive());
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							
						}						
						System.exit(0);
					}
				}).start();
		
			}
		});
		
		btnForceExit.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				// TODO Auto-generated method stub
				System.exit(0);
			}
		});
		
		
		
	}

	
	public class DateLabelFormatter extends AbstractFormatter {

		private String datePattern = "yyyy-MM-dd";
		private SimpleDateFormat dateFormatter = new SimpleDateFormat(datePattern);

		@Override
		public Object stringToValue(String text) throws ParseException {
			return dateFormatter.parseObject(text);
		}

		@Override
		public String valueToString(Object value) throws ParseException {

			if (value != null) {
				String dt = Constants.endDate;
				Calendar cal = (Calendar) value;
				Calendar today = Calendar.getInstance();
//System.out.println(dt);
				if (cal.getTime().after(dateFormatter.parse(dt))) {
					return dt;
				} else if (cal.getTime().after(today.getTime())) {
					return dateFormatter.format(today.getTime());
				} else {
					return dateFormatter.format(cal.getTime());
				}
			}
			return "";
		}
	}

	public class DateLabelFormatter2 extends AbstractFormatter {
		@Override
		public Object stringToValue(String text) throws ParseException {
			return dateFormatter.parseObject(text);
		}

		@Override
		public String valueToString(Object value) throws ParseException {
			if (value != null) {
				Calendar cal = (Calendar) value;
				Calendar today = Calendar.getInstance();
				if (cal.getTime().after(today.getTime())) {
					Constants.endDate = dateFormatter.format(today.getTime());
					return dateFormatter.format(today.getTime());
				} else {
					Constants.endDate = dateFormatter.format(cal.getTime());

					return dateFormatter.format(cal.getTime());
				}

			}
			return "";
		}

	}
	
	

	class UiUpdate extends Thread{
		public void run() {
			long timestamp =System.currentTimeMillis();
			while (Constants.keepRunning ) {
				//System.out.println("Running");
				if (((System.currentTimeMillis() - timestamp) / 1000) > 90) {
					Constants.previousDate =helper.getPreviousDayDate();					
					if(Constants.postfix.equals(Constants.previousDate)) {
						Constants.TotalProcessedRecord.set(0);
						Constants.postfix =helper.getTodayDate();
					}
					timestamp = System.currentTimeMillis();
				}
				if(Constants.keepRunning) {
					totlaFetched.setText(String.valueOf(Constants.TotalProcessedRecord.get()));
				}
				
				if(Constants.TotalPending==Constants.TotalProcessedRecord.get()) {
					appStatus.setText("Completed.");
					appStatus.setOpaque(false);
					Constants.keepRunning=false;
					btnStart.setEnabled(true);
					Constants.tableBucket.clear();
					textAreaLog.append("Completed");
					//System.out.println("tableReaderThread="+tableReaderThread.isAlive()+",deliveredInsertThread"+deliveredInsertThread.isAlive()+
					//		",failedInsertThread="+failedInsertThread.isAlive()+", uiUpdate="+uiUpdate.isAlive());
					Constants.executor.shutdown();
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}

