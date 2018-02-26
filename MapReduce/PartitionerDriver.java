 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.conf.Configured;
 import org.apache.hadoop.fs.Path;
 import java.io.IOException;
 import org.apache.hadoop.io.LongWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Mapper; 
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 import org.apache.hadoop.util.Tool;
 import org.apache.hadoop.util.ToolRunner; 
 import org.apache.hadoop.mapreduce.Reducer;
 import org.apache.hadoop.mapreduce.Partitioner;
 
public class PartitionerDriver extends Configured implements Tool{
	public static class AgePartitioner extends Partitioner <Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks){
			String[] nameAgeSalary = value.toString().split("\t");
			String age = nameAgeSalary[1];
			System.out.println("Inside partitioner");
			int ageInt = Integer.parseInt(age);
			if (ageInt <= 30)
			{
				System.out.println("Age in 0 is - " + ageInt);
				return 0 ;
			}
			if (ageInt > 30 && ageInt <= 50)
			{
				System.out.println("Age in 1 is - " + ageInt);
				return 1 ; // 1 % numReduceTasks
			}
			else
			{
				System.out.println("Age in 2 is - " + ageInt);
				return 2 ; // 2 % numReduceTasks
			}
		}
	}
	@Override
 	public int run(String[] args) throws Exception {
 		Configuration conf = new Configuration();
 		Job job = new Job(conf, "partitioner");
 
		job.setJarByClass(getClass());
		TextInputFormat.addInputPath(job, new Path(args[0]));
	 	job.setInputFormatClass(TextInputFormat.class);
 
		job.setMapperClass(PartitionerMapper.class);
 		job.setPartitionerClass(AgePartitioner.class);
	 	job.setReducerClass(PartitionerReducer.class);

 		job.setNumReduceTasks(3);

 		TextOutputFormat.setOutputPath(job, new Path(args[1]));
 		job.setOutputFormatClass(TextOutputFormat.class);
 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(Text.class);
 
		return job.waitForCompletion(true) ? 0 : 1;
 	}
 	public static void main(String[] args) throws Exception {
 	     int res = ToolRunner.run(new Configuration(), new PartitionerDriver(), args);
 	     System.exit(res);
 }	
	public static class PartitionerMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			String[] tokens = value.toString().split(",");
 			String gender = tokens[2].toString();
 			String nameAgeSalary = tokens[0]+"\t"+tokens[1]+"\t"+tokens[3];
			context.write(new Text(gender), new Text(nameAgeSalary));
 		}
 	} 

	public static class PartitionerReducer extends Reducer<Text, Text, Text, Text> {
 	@Override
 		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 		int maxSalary = Integer.MIN_VALUE;
		String name = " ";
		String age = " ";
 		String gender = " ";
 		int salary = 0;

 		for(Text val: values){
 			String [] valTokens = val.toString().split("\\t");
 			salary = Integer.parseInt(valTokens[2]);
			if(salary > maxSalary){
 				name = valTokens[0];
 				age = valTokens[1];
 				gender = key.toString();
 				maxSalary = salary;
 			}
 		}
 		context.write(new Text(name), new Text("age- "+age+"\t"+gender+"\tscore-"+maxSalary));
 		}
 	}
}

//Input Data
/*
Siv1,25,Male,25000
Ram1,30,Male,30000
Ash1,28,Female,40000
Uma1,24,Female,50000
Siv2,25,Male,25000
Ram2,30,Male,30000
Ash2,28,Female,40000
Uma2,24,Female,50000
Siv3,25,Male,25000
Ram3,55,Male,30000
Ash3,65,Female,40000
Uma3,75,Female,50000
Siv4,25,Male,25000
Ram4,30,Male,30000
Ash4,32,Female,40000
Uma4,35,Female,50000
*/


/*Output in 3 reducers
part-r-00000 (Every reducer receives only two records one with Male key and another with Female Key) for these two max salaried records are below.
Uma1	age- 24	Female	score-50000
Ram4	age- 30	Male	score-30000

part-r-00001
Uma4	age- 35	Female	score-50000

part-r-00002
Uma3	age- 75	Female	score-50000
Ram3	age- 55	Male	score-30000
*/
