
package com.cd.batchtool;

import java.util.Date;

import utils.MyTime;

public class StockPoint {
	private static final MyTime myTime=new MyTime();
	public Date date;
	public float s;
	public float e;
	public float min;
	public float max;
	public float volume;
	public float total;
	public StockPoint(float s, float e, float min, float max, float volume,
			float total,Date date) {
		this.s = s;
		this.e = e;
		this.min = min;
		this.max = max;
		this.volume = volume;
		this.total = total;
		this.date=date;
	}
	@Override
	public String toString(){
		return myTime.toLong(date)+","+s+","+max+","+min+","+e+","+volume+","+total;
	}
	
}
