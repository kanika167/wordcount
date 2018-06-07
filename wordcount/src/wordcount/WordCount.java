package wordcount;
import java.io.IOException;
import java.util.*;

//All these packages are present in hadoop-common.jar
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

//All these packages are present in hadoop-mapreduce-client-core.jar
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapred.Mapper;

public class WordCount {
public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {
			
 //mapper stores output in context
			
				String line = value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line);
				
				while (tokenizer.hasMoreTokens()) {
					value.set(tokenizer.nextToken());
					context.write(value, new IntWritable(1));
				}
				
			}
		}
		
	
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int sum=0;
			
			for (IntWritable x : value) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum));
		
	}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,"WordCount");
		
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
