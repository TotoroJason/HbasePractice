package com.hbase.examples;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ImportFromFile {
	public static final String NAME = "ImportFromFile";
	public enum Counters {LINES}
	
	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text>{
		private byte[] family = null;
		private byte[] qualifier = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			String column = context.getConfiguration().get("conf.column");
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			
			family = colkey[0];
			if(colkey.length > 1){
				qualifier = colkey[1];
			}			
		}
		
		@Override
		public void map(LongWritable offset, Text line, Context context)
		throws IOException{
			try{
				String lineStr = line.toString();
				byte[] rowkey = DigestUtils.md5(lineStr);
				
		//		Put put = new Put(rowkey);
		//		put.add(family, qualifier, Bytes.toBytes(lineStr));
		//		String srt2=new String(rowkey,"UTF-8");
				context.write(new ImmutableBytesWritable(rowkey),new Text(lineStr));
			}
			catch (Exception e){
				System.err.println(e.toString());
			}
		}
		
	}
	
	static class MyReducer extends TableReducer<ImmutableBytesWritable, Text, NullWritable>
	{
		private byte[] family = null;
		private byte[] qualifier = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			String column = context.getConfiguration().get("conf.column");
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
			
			family = colkey[0];
			if(colkey.length > 1){
				qualifier = colkey[1];
			}			
		}
		
		@Override
		public void reduce(ImmutableBytesWritable k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException
		{
			for(Text v2 : v2s)
			{
				Put put = new Put(k2.get());
				put.add(family, qualifier, Bytes.toBytes(v2.toString()));
				context.write(NullWritable.get(),put);
				
			}
		}
	}
	
	private static CommandLine parseArgs(String[] args) throws ParseException{
		Options options = new Options();
		Option o = new Option("t", "table", true, "table to import into (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("c", "column", true, "column to store row data into (must exist)");
		o.setArgName("family:qualifier");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("i", "input", true, "the directory or file to read from");
		o.setArgName("path-in-HDFS");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option("d", "debug", true, "switch on DEBUG log level");
		options.addOption(o);
		
		CommandLineParser parser =new PosixParser();
		CommandLine cmd = null;
		
		try{
			cmd = parser.parse(options, args);
		}catch (Exception e){
			System.err.println("Error: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		return cmd;
	}
	
	public static void main(String[] args) throws Exception {
		for(int i=0; i<args.length; i++)
			  System.err.println("args: " + args[i] + "\n");
		
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String table = cmd.getOptionValue("t");
		String input = cmd.getOptionValue("i");
		String column = cmd.getOptionValue("c");
		conf.set("conf.column", column);
		conf.set(TableOutputFormat.OUTPUT_TABLE, table);
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Import from file "+ input + " into table " + table);
		
		job.setJarByClass(ImportFromFile.class);
		job.setMapperClass(ImportMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        
        
		job.setOutputFormatClass(TableOutputFormat.class);

		
		FileInputFormat.addInputPath(job,  new Path(input));
		
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
	}
	
	
	
	
}
