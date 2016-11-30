package com.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;



public class ScanExample
{

	private static Configuration conf =null;  
	static {  
        conf = HBaseConfiguration.create();  
    }  
	
	
	public static void  main (String [] agrs) 
	{  
		ResultScanner scanner1 = null;
		try
		{
			HTable table = new HTable(conf, "testTable");  
			Scan scan1 = new Scan();
			scanner1 = table.getScanner(scan1);
			
			for(Result res : scanner1)
			{
				System.out.println(res);
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
