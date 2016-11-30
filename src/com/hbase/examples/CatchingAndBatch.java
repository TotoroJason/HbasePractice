package com.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class CatchingAndBatch {
	private static void scan(int caching, int batch) throws IOException
	{
		Logger log = Logger.getLogger("org.apache.hadoop");
		final int[] counters = {0,0};
		Appender appender = new AppenderSkeleton(){
			@Override
			protected void append(LoggingEvent event)
			{
				String msg = event.getMessage().toString();
				if(msg != null && msg.contains("Call: next"))
				{
					counters[0]++;
				}
			}
			@Override
			public void close() {}
			@Override
			public boolean requiresLayout(){
				return false;
			}
		};
		
		log.removeAllAppenders();
		log.setAdditivity(false);
		log.addAppender(appender);
		log.setLevel(Level.DEBUG);
		
		Scan scan = new Scan();
		scan.setCaching(caching);
		scan.setBatch(batch);
		
		String tableName = "testTable";
		Configuration conf = HBaseConfiguration.create(); 
		HTable table = new HTable(conf, tableName);  
		
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner)
		{
			counters[1]++;
		}
		scanner.close();
		
		System.out.println("Caching: " + caching + ", Batch: " + batch +
				", Results: " + counters[1] + ", RPCs: " + counters[0]);
		
	}
	public static void main(String[] args) throws IOException
	{
		scan(1,1);
		scan(200,1);
		scan(2000,100);
		scan(2,100);
		scan(2,10);
		scan(5,100);
		scan(5,20);
		scan(10,20);
		
	}
}
