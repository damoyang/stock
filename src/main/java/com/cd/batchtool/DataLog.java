package com.cd.batchtool;

import java.io.*;
import java.util.*;

public class DataLog {
	static DataLog single;
	static synchronized DataLog getInstance() throws IOException{
		if(single==null){
			single=new DataLog();
		}
		return single;
	}
	
	String log = "log.f";
	File f;
	BufferedReader br;
	HashSet<String> hashSet;

	private DataLog() throws IOException {
		f = new File(log);
		if (!f.exists()) {
			f.createNewFile();
		}
		br = new BufferedReader(new FileReader(f));
		hashSet = new HashSet<String>();
		String line=br.readLine();
		while(line!=null){
			hashSet.add(line);
			line=br.readLine();
		}
		br.close();
	}
	public boolean isFinished(String file) {
		return hashSet.contains(file);
	}
	public synchronized void add(String file) throws IOException{
		hashSet.add(file);
		flush();
	}
	public void close() throws IOException{
		BufferedWriter bw=new BufferedWriter(new FileWriter(f));
		for(String key:hashSet){
			bw.write(key);
			bw.newLine();
			bw.flush();
		}
	}
	public void flush() throws IOException{
		BufferedWriter bw=new BufferedWriter(new FileWriter(f));
		for(String key:hashSet){
			bw.write(key);
			bw.newLine();
			bw.flush();
		}
	}
}
