package test.mr.project;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// As shown below if we provide cache file through job.addCacheFile() method, then this URI should be complete HDFS file URI including (hdfs://namenode:8020/file/path).
// In this case the command to run the program.
// $ yarn jar WC.jar WordCountJob mr_input.txt mr_out
// If we do not specify our input file through job.addCacheFile() method, then we can provide through command line as shown below
// but the filename should match the linkname that used in below mapper. And the driver program must implement Tool interface as shown below.
// $ yarn jar WC.jar WordCountJob -files hdfs://localhost:9000/input/path/Exclude_File mr_input.txt mr_out

public class WordCount_DistCache implements Tool {

	private Configuration conf;
	public void setConf(Configuration conf) {

		this.conf = conf;
	}
	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WordCountJOb");
		job.setJarByClass(WordCount_DistCache.class);

		job.setMapperClass(WordCountMapper.class);

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.addCacheFile(new URI("hdfs://localhost:9000/user/hadoop1/Exclude_File#Exclude_File.txt"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCount_DistCache(), args);
		System.exit(exitCode);
	}
}


