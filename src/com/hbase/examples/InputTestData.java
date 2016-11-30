package com.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InputTestData {

	static public String intToString(int x){  
        String result=String.valueOf(x);  
        int size=result.length();  
        while(size<7){  
            size++;  
            result="0"+result;  
        }  
        result = "row-" + result;
        return result;  
    }  
	public static void inputDataToHBase() throws Exception
	{
		String tableName = "testTable";
		String[] familyNames = {"family1", "family2"};
		String columnNamePrefix = "column";
		
		Configuration conf = HBaseConfiguration.create(); 
		HBaseAdmin admin = new HBaseAdmin(conf);     
        if (admin.tableExists(tableName))
        {     
            System.out.println("table already exists!");     
        } 
        else 
        {     
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);  
            for(int i=0; i<familyNames.length; i++)
            {
            	HColumnDescriptor hdc = new HColumnDescriptor(familyNames[i]);
            	tableDesc.addFamily(hdc);
            }
            
            long before = System.currentTimeMillis();
            admin.createTable(tableDesc);
            //admin.createTable(tableDesc,Bytes.toBytes("0000000"),Bytes.toBytes("9999999"),150); 
            long after = System.currentTimeMillis();
            long interval = after-before;
            System.out.println("createTable time: " + interval);
            
            HTable table = new HTable(conf, tableDesc.getName());
            table.setAutoFlush(false);
            //table.setWriteBufferSize(209715200);
            System.out.println("Write Buffer Size: " + table.getWriteBufferSize());
            
            long begin = System.currentTimeMillis();
            for(int j=0; j<familyNames.length; j++)
            {
            	String familyName = familyNames[j];
            	for(int i=0; i<10; i++)
                {
                	
                	String cowName = intToString(i);
                	for(int k=0; k<10; k++)
                	{
                		String value = "value-" + i + "-" +k;
                		Put p1 = new Put(Bytes.toBytes(cowName));
                    	p1.setWriteToWAL(false);
                    	p1.add(Bytes.toBytes(familyName), Bytes.toBytes(columnNamePrefix+k), 
                    			Bytes.toBytes(value));
                    	table.put(p1);
                	}
                	
                }
            			
            }
            
            long end = System.currentTimeMillis();
            table.flushCommits();
            System.out.println("Write data time: "+ (end - begin));
            
            System.out.println("create table " + tableName + " ok.");     
        }      
	}
	
	public static void main(String []args) throws IOException
	{  
		try {
			inputDataToHBase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }  
}
