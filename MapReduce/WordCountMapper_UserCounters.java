package test.mr.project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper_UserCounters extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final Text result = new Text();

	enum Tokens {
		Total, FirstCharUpper, FirstCharLower
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] words = value.toString().split(" ");
		for(String word: words) {

			context.write(new Text(word), new IntWritable(1));

			context.getCounter(Tokens.Total).increment(1);
			char firstChar = token.charAt(0);
			if ( Character.isUpperCase(firstChar)){
				context.getCounter(Tokens.FirstCharUpper).increment(1);
			} else {
				context.getCounter(Tokens.FirstCharLower).increment(1);
			}
		}
	}
}
