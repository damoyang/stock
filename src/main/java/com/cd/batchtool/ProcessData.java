package com.cd.batchtool;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

import utils.FileProcess;
import utils.MyTime;

public class ProcessData implements Runnable {
	public static class FileDate {
		public File f;
		public Date s;

		public FileDate(File f, Date s) {
			this.f = f;
			this.s = s;
		}
	}
	String stock;
	List<File> flist;
	List<FileDate> fdlist = new ArrayList<FileDate>();
	MyTime myTime = new MyTime();
	String[] times={"5min","15min","30min","1h","1day","1week","1month","1year"};
	Map<String,TreeMap<Long,StockPoint>> maps=new HashMap<String, TreeMap<Long, StockPoint>>();
	public ProcessData(String stock, List<File> flist) {
		this.stock = stock;
		this.flist = flist;
		for(String t:times){
			maps.put(t,new TreeMap<Long,StockPoint>());
		}
	}

	public void run() {
		System.out.println("Processing:"+stock);
		for (File f : flist) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(f));
				String firstLine = br.readLine();
				if (firstLine == null) {
					continue;
				}
				String[] values = firstLine.split(",");
				if (values.length != 8) {
					System.out.println("ERROR In" + f.getName());
					continue;
				}
				Date date = myTime.toDate(values[0] + values[1]);
				if (date == null) {
					System.out.println("ERROR In" + f.getName());
					continue;
				}
				fdlist.add(new FileDate(f, date));
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Collections.sort(fdlist, new Comparator<FileDate>(){
			public int compare(FileDate o1, FileDate o2) {
				return o1.s.compareTo(o2.s);
			}
		});
		for(FileDate fd:fdlist){
			try{
				process(fd.f);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		for(String dir:times){
			File f=new File(dir);
			if(!f.isDirectory()){
				f.mkdir();
			}
			String path=f.getAbsolutePath()+File.separator+stock;
			TreeMap<Long,StockPoint> tm=maps.get(dir);
			try {
				BufferedWriter bw=new BufferedWriter(new FileWriter(path));
				for(Long key:tm.keySet()){
					StockPoint sp=tm.get(key);
					bw.write(myTime.toLong(new Date(key))+","+sp.toString());
					bw.newLine();
					bw.flush();
				}
				bw.flush();
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			DataLog dl = DataLog.getInstance();
			dl.add(stock);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void process(File file) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String stringLine = "";
		String[] values;
		// start to process the file.
		while ((stringLine = bufferedReader.readLine()) != null) {
			values = stringLine.trim().split(",");
			if (values.length != 8) {
				System.out.println("ERROR In" + stringLine);
				break;
			}
			for(int i=0;i<times.length;i++){
				StockPoint sp=new StockPoint(Float.parseFloat(values[2]),Float.parseFloat(values[5]),Float.parseFloat(values[4]),Float.parseFloat(values[3]),Float.parseFloat(values[6]),Float.parseFloat(values[7]),myTime.toDate(values[0] + values[1]));
				updateMaps(i,sp);
			}
 		}
		bufferedReader.close();
	}
	private void updateMaps(int i,StockPoint sp){
		TreeMap<Long,StockPoint> tm=maps.get(times[i]);
		Long t=myTime.getLasterDate(sp.date,i);
		if(tm.containsKey(t)){
			StockPoint line=tm.get(t);
			line.min=Math.min(line.min,sp.min);
			line.max=Math.max(line.max,sp.max);
			line.volume+=sp.volume;
			line.total+=sp.total;
			line.e=sp.e;
		}else{
			sp.date=new Date(t);
			tm.put(t,sp);
		}	
	}
	public static void main(String[] args) throws IOException {
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
			Thread t = new Thread(new ProcessData(stock, filesMap.get(stock)));
			es.execute(t);
		}
		es.shutdown();

	}

}
