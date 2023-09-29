package org.tdot.threads;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.tdot.main.LatencyReportUi;
import org.tdot.utils.Constants;


public class LoggingThread extends Thread {
	SimpleDateFormat dtFormat1 = new SimpleDateFormat("yyyy-MM-dd");
	SimpleDateFormat dtFormat = new SimpleDateFormat("HH");
	int tmpcounter = 0;
	long timestamp;
	String filePath = "";
	String updatefilePath = "";
	public boolean isRunning = true;
	Date date = new Date();

	public void run() {
		timestamp = System.currentTimeMillis();
		while (isRunning) {
			try {
				Thread.sleep(1000);
				if (Constants.consolelog.size() > 0) {
					// System.out.println("In generate error");
					generateErrorReport();
				}
				
						

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private Boolean checkEmpty() {
		if (Constants.consolelog.size() == 0) {

			return true;
		} else {

			return false;
		}

	}
	
	
	private void generateErrorReport() {
		String data = "";
		Date date = new Date();
		String projPath = "";
		try {
			projPath = new StringBuilder().append(new File("").getCanonicalPath()).append(File.separator)
					.append("errorlogs").append(File.separator).append(dtFormat1.format(date)).append(File.separator)
					.toString();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (checkEmpty()) {
		} else {
			File directory = new File(projPath);
			if (!directory.exists()) {
				// System.out.println("Directory not exist");
				directory.mkdirs();
			}
			try {
				File file = new File(projPath + "LogHour_" + dtFormat.format(date) + ".txt");
				if (!file.exists()) {
					try {
						file.createNewFile();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				BufferedWriter out;
				out = new BufferedWriter(new FileWriter(file, true));
				while (!Constants.consolelog.isEmpty()) {
					data = Constants.consolelog.peek();
					LatencyReportUi.textAreaLog.append(data + "\n");// setText(data);
					out.write(data + " \n\n");
					Constants.consolelog.remove(data);
				}

				out.close();
			} catch (IOException ie) {
				System.out.println("Error in creating errorlog file.");
				ie.printStackTrace();
			}

		}

	}
/*
	private void generateDebugReport() {
		String data = "";
		Date date = new Date();
		String projPath = "";
		try {
			projPath = new StringBuilder().append(new File("").getCanonicalPath()).append(File.separator)
					.append("debuglogs").append(File.separator).append(dtFormat1.format(date)).append(File.separator)
					.toString();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		File directory = new File(projPath);
		if (!directory.exists()) {
			// System.out.println("Directory not exist");
			directory.mkdirs();
		}
		try {
			File file = new File(projPath + "LogHour_" + dtFormat.format(date) + ".txt");
			if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			BufferedWriter out;
			out = new BufferedWriter(new FileWriter(file, true));
			while (!Constants.debuglogBucket.isEmpty()) {
				data = Constants.debuglogBucket.peek();
				out.write(data + " \n\n");
				Constants.debuglogBucket.remove(data);
			}

			out.close();
		} catch (IOException ie) {
			System.out.println("Error in creating debugLog file.");
			ie.printStackTrace();
		}

	}
*/
	
}
