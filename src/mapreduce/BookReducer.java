package mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class BookReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
	private final static byte[] RESULT_CF = Bytes.toBytes("res");
	
	@Override
	protected void reduce(Text symbol, Iterable<Text> symbolBooks, Context context)
			throws IOException, InterruptedException
	{
		StringBuffer booksString = new StringBuffer();
		Put put = new Put(Bytes.toBytes(symbol.toString()));
		
		for(Text book : symbolBooks) {
			booksString.append(book.toString()).append("\n");
		}
		
		put.add(RESULT_CF, Bytes.toBytes("bookBySymbol"), 
				Bytes.toBytes(booksString.toString()));
		context.write(null, put);
	}

}
