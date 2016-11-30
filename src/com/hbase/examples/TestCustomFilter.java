package com.hbase.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.examples.CustomFilter;

public class TestCustomFilter {

	public static void  main (String [] agrs) 
	{
		ResultScanner scanner1 = null;
		try
		{
			Configuration conf = HBaseConfiguration.create(); 
			HTable table = new HTable(conf, "testTable");  
			
			List<Filter> filters = new ArrayList<Filter>();
			
			Filter filter1 = new CustomFilter(Bytes.toBytes("value-1"));
			filters.add(filter1);
			
			Filter filter2 = new CustomFilter(Bytes.toBytes("value-3-5"));
			filters.add(filter2);
			
			Filter filter3 = new CustomFilter(Bytes.toBytes("value-7"));
			filters.add(filter3);
			
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
			
			Scan scan1 = new Scan();
			scan1.setFilter(filterList);
			scanner1 = table.getScanner(scan1);
			
			for(Result res : scanner1)
			{
				for(KeyValue kv : res.raw())
				{
					System.out.println("KV: "+kv + ", Value: "+ Bytes.toString(kv.getValue()));;
				}
			}			
		}
		catch(IOException e){     
            e.printStackTrace();     
        }     
		finally
		{
			if(scanner1!=null)
				scanner1.close();
		}
	}
}
