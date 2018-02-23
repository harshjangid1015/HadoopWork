package test.mr.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class WordCountMapper_DistCache extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public Logger log = Logger.getLogger(WordCountMapper_DistCache.class);

	public final static String EXCLUDE_FILE = "Exclude_File.txt";

	public final Set<String> excludeSet = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

			FileReader reader = new FileReader(new File(EXCLUDE_FILE));
			BufferedReader bufferedReader = null;
			try {
				bufferedReader = new BufferedReader(reader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					excludeSet.add(line.trim());
				}
			} finally {
				reader.close();
				bufferedReader.close();
			}

	}

	enum Tokens {
		Total, FirstCharUpper, FirstCharLower
	}


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 String line = value.toString();
		 String[] words = line.split(" ");
		 for(String word: words)
		 {
			char firstChar = word.charAt(0);
			System.out.println(" FirstChar is " + firstChar);

			if (!excludeSet.contains(Character.toString(firstChar)))
			{
				context.write(new Text(word), new IntWritable(1));

				context.getCounter(Tokens.Total).increment(1);
				if (Character.isUpperCase(firstChar))
				{
					context.getCounter(Tokens.FirstCharUpper).increment(1);
				}
				else
				{
					context.getCounter(Tokens.FirstCharLower).increment(1);
				}
			}
		 }
	}
}
