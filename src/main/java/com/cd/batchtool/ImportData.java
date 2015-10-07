package com.cd.batchtool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import utils.FileProcess;
import utils.MyTime;

import com.cd.cassandra.CassandraCluster;

public class ImportData extends Thread
{

	private CassandraCluster iostream = CassandraCluster.getInstance();

	private MyTime myTime = new MyTime();

	private String ks = "stockdatas";
	private String kline = "stockkbar_one_minute";
	private String khigh = "stockkbar_one_minute_high";
	private String klow = "stockkbar_one_minute_low";
	private String kopen = "stockkbar_one_minute_open";
	private String kclose = "stockkbar_one_minute_close";
	private String kvolumn = "stockkbar_one_minute_volumn";
	private String ktotal = "stockkbar_one_minute_total_mon";
	private String row;
	private long time;
	private String asLine;
	private List<File> files;
	private String stock;
	public ImportData(String stock,List<File> files) {
		this.stock=stock;
		this.files = files;
	}

	public void run() {
		for (File file : files) {
			try {
				process(file);
			}
			catch (Exception e) {
				e.printStackTrace();
				return ;
			}
		}
		try {
			DataLog dl=DataLog.getInstance();
			dl.add(stock);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	 public static String getFileNameNoEx(String filename) { 
	        if ((filename != null) && (filename.length() > 0)) { 
	            int dot = filename.lastIndexOf('.'); 
	            if ((dot >-1) && (dot < (filename.length()))) { 
	                return filename.substring(0, dot); 
	            } 
	        } 
	        return filename; 
	    }
	private void process(File file) throws Exception {
		row = getFileNameNoEx(file.getName());
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String stringLine = "";
		String[] values;
		System.out.println(this.getName()+"has started to import "+file.getName());
		// start to process the file.
		int counter=0;
		while ((stringLine = bufferedReader.readLine()) != null) {
			if(counter>1000){
				SpeedLog sl=SpeedLog.getInstance();
				sl.add();
				counter=counter%1000;
			}
			values = stringLine.trim().split(",");
			if (values.length != 8) {
				System.out.println("ERROR In"+stringLine);
				break;
			}
			time = myTime.toLong(values[0] + values[1]);
			asLine = values[2] + "," + values[3] + "," + values[4] + "," + values[5] + "," + values[6] + ","
					+ values[7];
			iostream.insertLineData(ks, kline, row, time, asLine);
			iostream.insertData(ks, kopen, row, time, Float.parseFloat(values[2]));
			iostream.insertData(ks, khigh, row, time, Float.parseFloat(values[3]));
			iostream.insertData(ks, klow, row, time, Float.parseFloat(values[4]));
			iostream.insertData(ks, kclose, row, time, Float.parseFloat(values[5]));
			iostream.insertData(ks, kvolumn, row, time,Float.parseFloat(values[6]));
			iostream.insertData(ks, ktotal, row, time, Float.parseFloat(values[7]));
			counter+=7;
		}
		FileProcess.rename(file);
		bufferedReader.close();
		System.out.println(this.getName()+"has finished import file "+file.getName());
	}

	public static void main(String[] args) throws Exception {
		Map<String, List<File>> filesMap = new HashMap<String, List<File>>();
		List<File> files = FileProcess.getFiles(args[0]);
		DataLog dl=DataLog.getInstance();
		int threadNum = Integer.parseInt(args[1]);
		for (File f : files) {
			if(dl.isFinished(f.getName())) continue;
			if (!filesMap.containsKey(f.getName())) {
				filesMap.put(f.getName(), new ArrayList<File>());
			}
			filesMap.get(f.getName()).add(f);
		}
		ExecutorService es = Executors.newFixedThreadPool(threadNum);
		for (String stock : filesMap.keySet()) {
			Thread t = new Thread(new ImportData(stock, filesMap.get(stock)));
			es.execute(t);
		}
		es.shutdown();
	}
}