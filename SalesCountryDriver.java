package sales;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SalesCountryDriver {
	public static void main(String[] args) throws Exception {
		
		JobConf job = new JobConf(SalesCountryDriver.class);

		job.set("fs.file.impl", "sales.WinLocalFileSystem");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(SalesMapper.class);
		job.setReducerClass(SalesCountryReducer.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
			JobClient.runJob(job);
	}
}
