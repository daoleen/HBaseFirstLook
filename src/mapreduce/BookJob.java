package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class BookJob {
	private final static String inputTable = "books";
	private final static String outputTable = "books";
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		
		/*
		// Creating the output table
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		HTableDescriptor outputDescr = new HTableDescriptor(outputTable);
		HColumnDescriptor outputColumn = new HColumnDescriptor("res");
		outputDescr.addFamily(outputColumn);
		hadmin.createTable(outputDescr);
		*/
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("b"), Bytes.toBytes("name"));
		
		Job job = new Job(conf, "Working with HBase");
		job.setJarByClass(BookJob.class);
		
		TableMapReduceUtil.initTableMapperJob(inputTable, scan, BookMapper.class, 
				Text.class, Text.class, job);
		
		TableMapReduceUtil.initTableReducerJob(outputTable, BookReducer.class, job);
		
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);
		
		job.setNumReduceTasks(2);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
