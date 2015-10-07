package com.cd.batchtool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

public class SpeedLog {
	static SpeedLog single;

	static synchronized SpeedLog getInstance() throws IOException {
		if (single == null) {
			single = new SpeedLog();
		}
		return single;
	}

	Timer timer;

	String log = "speed_log.f";
	File f;
	AtomicLong counter = new AtomicLong();
	long pre=0;
	private SpeedLog() throws IOException {
		f = new File(log);
		if (!f.exists()) {
			f.createNewFile();
		}
		timer=new Timer();
		timer.schedule(new TimerTask(){

			@Override
			public void run() {
				long cur=counter.get();
				System.out.println("THE SPEED IS:"+(cur-pre)+"K/s");
				pre=cur;
			}
			
		},0L,1000L);
	}

	public void add() {
		counter.getAndIncrement();
	}

}
