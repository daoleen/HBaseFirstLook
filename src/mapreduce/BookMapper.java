package mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class BookMapper extends TableMapper<Text, Text> {
	private final static byte[] BOOK_CF = Bytes.toBytes("b");
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
	{
		String id = Bytes.toString(value.getRow()).substring(0, 1);
		String bookName = Bytes.toString(value.getValue(BOOK_CF, Bytes.toBytes("name")));
		context.write(new Text(id), new Text(bookName));
	}
}
