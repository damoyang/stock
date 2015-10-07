package utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.text.ParseException;

import com.sun.corba.se.spi.orb.StringPair;

/**
 * Time Class from String to Long
 * 
 * @author KangRong
 * 
 */
public class MyTime {
	private Calendar calendar=Calendar.getInstance();
	private SimpleDateFormat sdf;
	private SimpleDateFormat tosdf;

	public MyTime() {
		sdf = new SimpleDateFormat("yyyy/MM/ddHH:mm");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		tosdf = new SimpleDateFormat("yyyyMMddHHmmss");
		tosdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
	}

	/**
	 * transform format long Object to string(yyyyMMddHHmmssSSS)
	 * 
	 * @param timeL
	 * @return
	 */
	public String toString(long time) {
		Date dt = new Date(time);
		return sdf.format(dt);
	}

	/**
	 * transform format string(yyyy/MM/ddHH:mm) to long Object
	 * 
	 * @param timeString
	 * @return
	 */
	public Long toLong(String time) {
		Date date = new Date();
		try {
			date = sdf.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Long.parseLong(tosdf.format(date));
	}
	public Long toLong(Date date) {
		return Long.parseLong(tosdf.format(date));
	}
	public Date toDate(String time) {
		Date date = null;
		try {
			date = sdf.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}

	// mode:
	// 0 5min
	// 1 15min
	// 2 30min
	// 3 1h
	// 4 1d
	// 5 1week
	// 6 1month
	// 7 1year
	private long[] timeMap = { 5 * 60 * 1000, 15 * 60 * 1000, 30 * 60 * 1000,
			60 * 60 * 1000 };
	private long ONE_DAY = 24 * 60 * 60 * 1000;

	public Long getLasterDate(Date date, int mode) {
		long curTime = date.getTime();
		if (mode == 7) {
			return getYearFirst(date).getTime();
		} else if (mode == 6) {
			return getMonthFirst(date).getTime();
		} else if (mode == 5) {
			return getWeekFirst(date).getTime();
		} else if (mode == 4) {
			return curTime-curTime%ONE_DAY;
		} else {
			long time = timeMap[mode];
			if (curTime % time == 0)
				return curTime;
			else {
				return (curTime / time + 1) * time;
			}
		}
	}

	/**
	 * 获取某年第一天日期
	 * 
	 * @param year
	 *            年份
	 * @return Date
	 */
	public Date getYearFirst(Date date) {
		try{
		calendar.setTime(date);
		int year=calendar.get(Calendar.YEAR);
		calendar.clear();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.DAY_OF_YEAR,1);
		Date currYearFirst = calendar.getTime();
		return currYearFirst;
		}catch(Exception e){
			System.out.println(date);

		}
		return date;
	}

	/**
	 * 获取某年最后一天日期
	 * 
	 * @param year
	 *            年份
	 * @return Date
	 */
	public Date getYearLast(Date date) {
		calendar.setTime(date);
		int year=calendar.get(Calendar.YEAR);
		calendar.clear();
		calendar.set(Calendar.YEAR, year);
		calendar.roll(Calendar.DAY_OF_YEAR, -1);
		Date currYearLast = calendar.getTime();
		return currYearLast;
	}
	public Date getMonthFirst(Date date){
		calendar.setTime(date);
		int year=calendar.get(Calendar.YEAR);
		int month=calendar.get(Calendar.MONTH);
		calendar.clear();
		calendar.set(Calendar.YEAR,year);
		calendar.set(Calendar.MONTH,month);
		return calendar.getTime();
	}
	public Date getMonthLast(Date date){
		calendar.setTime(date);
		int year=calendar.get(Calendar.YEAR);
		int month=calendar.get(Calendar.MONTH);
		calendar.clear();
		calendar.set(Calendar.YEAR,year);
		calendar.set(Calendar.MONTH,month);
		calendar.roll(Calendar.DAY_OF_MONTH,-1);
		return calendar.getTime();
	}
	public Date getWeekFirst(Date date){
		calendar.setTime(date);
		int year=calendar.get(Calendar.YEAR);
		int month=calendar.get(Calendar.MONTH);
		int week=calendar.get(Calendar.WEEK_OF_MONTH);
		calendar.clear();
		calendar.set(Calendar.YEAR,year);
		calendar.set(Calendar.MONTH,month);
		calendar.set(Calendar.WEEK_OF_MONTH,week);
		calendar.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
		return calendar.getTime();
	}
}
