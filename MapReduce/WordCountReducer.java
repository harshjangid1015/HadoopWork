package test.mr.project;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	Logger log = Logger.getLogger(WordCountMapper.class);
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum+= value.get();
		}
		context.write(key, new IntWritable(sum));

	}
}
