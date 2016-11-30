package com.hbase.examples;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomFilter extends FilterBase {
	private byte[] value = null;
	private boolean filterRow = true;
	
	public CustomFilter()
	{
		super();
	}
	public CustomFilter(byte[] value)
	{
		this.value = value;
	}
	@Override
	public void reset()
	{
		this.filterRow = true;
	}
	public ReturnCode filterKeyValue(KeyValue kv)
	{
		if(Bytes.compareTo(value, kv.getValue()) == 0)
		{
			filterRow = false;
		}
		return ReturnCode.INCLUDE;
	}
	@Override
	public ReturnCode filterKeyValue(Cell arg0) throws IOException 
	{
		if(Bytes.compareTo(value, arg0.getValue()) == 0)
		{
			filterRow = false;
		}
		return ReturnCode.INCLUDE;
	}
	@Override
	public boolean filterRow()
	{
		return filterRow;
	}

	public void write(DataOutput dataOutput) throws IOException
	{
		Bytes.writeByteArray(dataOutput, this.value);
	}
	public void readFields(DataInput dataInput) throws IOException
	{
		this.value = Bytes.readByteArray(dataInput);
	}
	
}
