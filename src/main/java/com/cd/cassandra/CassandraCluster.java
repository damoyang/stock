package com.cd.cassandra;

import java.net.InetAddress;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraCluster
{
    private static CassandraCluster single;
	private String node = "192.168.1.191";
	private Cluster cluster;
	private Session session;
	
	private String createKsCql = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'%s', 'replication_factor':%d};";
	private String createCfCql = "CREATE TABLE IF NOT EXISTS %s.%s (" + "codemarket text," + "datetime bigint,"
			+ "value float," + "PRIMARY KEY(codemarket,datetime)" + ");";
	private String createCflineCql = "CREATE TABLE IF NOT EXISTS %s.%s (" + "codemarket text," + "datetime bigint,"
			+ "value text," + "PRIMARY KEY(codemarket,datetime)" + ");";
	private String deleteCql = "delete value from %s.%s where codemarket='%s' and datetime=%d;";
	private String selectCql = "select * from %s.%s where codemarket='%s' and datetime=%d;";
	private String selectCountCql = "select count(*) from %s.%s where codemarket ='%s' and datetime > %d and datetime <= %d;";
	private String selectcodemarketCql = "select codemarket from %s.%s limit 1000000;";

	private String ks = "stockdatas";
	private String kline = "stockkbar_one_minute";
	private String khigh = "stockkbar_one_minute_high";
	private String klow = "stockkbar_one_minute_low";
	private String kopen = "stockkbar_one_minute_open";
	private String kclose = "stockkbar_one_minute_close";
	private String kvolumn = "stockkbar_one_minute_volumn";
	private String ktotal = "stockkbar_one_minute_total_mon";

	public CassandraCluster() {
		connect(node);
		createKs(ks, "SimpleStrategy", 3);
		createCf(ks, klow);
		createCf(ks, khigh);
		createCf(ks, kopen);
		createCf(ks, kclose);
		createCf(ks, kvolumn);
		createCf(ks, ktotal);
		createCf(ks, kline,createCflineCql);
	}
	public synchronized static CassandraCluster getInstance(){
		if(single==null){
			single=new CassandraCluster();
		}
		return single;
	}

	private void connect(String node) {
		try {
			cluster = Cluster.builder().addContactPoints(InetAddress.getByName(node)).build();
			session = cluster.connect();
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public static void close() {
		single.session.close();
		single.cluster.close();
	}

	public void createKs(String ks, String strategy, int replication) {
		try {
			session.execute(String.format(createKsCql, ks, strategy, replication));
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void createCf(String ks, String cf) {
		try {
			session.execute(String.format(createCfCql, ks, cf));
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
	public void createCf(String ks, String cf, String sql) {
		try {
			session.execute(String.format(sql, ks, cf));
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public ResultSet insertData(String ks, String cf, String codemarket, long datetime, float value) {
		Statement statement = QueryBuilder.insertInto(ks, cf).value("codemarket", codemarket).value("datetime", datetime)
				.value("value", value);
		return session.execute(statement);
	}
	
	public ResultSet insertLineData(String ks, String cf, String codemarket, long datetime, String value) {
		Statement statement = QueryBuilder.insertInto(ks, cf).value("codemarket", codemarket).value("datetime", datetime)
				.value("value", value);
		return session.execute(statement);
	}

	public ResultSet selectData(String ks, String cf, String codemarket, long datetime) {
		System.out.println(String.format(selectCql, ks, cf, codemarket, datetime));
		return session.execute(String.format(selectCql, ks, cf, codemarket, datetime));
	}

	public long selectCount(String ks, String cf, String codemarket, long starttimestamp, long endtimestamp) {
		long count = 0;
		try {
			ResultSet rSet = session.execute(String.format(selectCountCql, ks, cf, codemarket, starttimestamp,
					endtimestamp));
			if (rSet == null)
				return count;
			Row one = rSet.one();
			if (one == null)
				return count;
			count = one.getLong("count");
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
		return count;
	}

	public ResultSet deleteData(String ks, String cf, String codemarket, String datetime) {
		ResultSet rSet = null;
		try {
			rSet = session.execute(String.format(deleteCql, ks, cf, codemarket, datetime));
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
		return rSet;
	}

	public ResultSet selectcodemarketAll(String ks, String sensorId) {
		ResultSet rSet = null;
		try {
			rSet = session.execute(String.format(selectcodemarketCql, ks, sensorId));
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
		return rSet;
	}

}
