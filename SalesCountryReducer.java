package sales;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		
		int frequency=0;
		while (values.hasNext()) {
			IntWritable value = (IntWritable) values.next();
			frequency+= value.get();
		}
		output.collect(key, new IntWritable(frequency));
	}
}
