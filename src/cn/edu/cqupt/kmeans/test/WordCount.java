package cn.edu.cqupt.kmeans.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.MultipleTextFileInputFormat;

public class WordCount {
	
	
	public static class Wmap extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Log Log = LogFactory.getLog(Wmap.class);
		private final static LongWritable one = new LongWritable(1);
	    private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Log.info("line "+line);
			String[] strs = line.split(" ");
			for (String string : strs) {
				word.set(string);
				context.write(word, one);
			}
		}
		
	}
	
	public static class FileReadMapper extends Mapper<Text, Text, Text, LongWritable>{

		@Override
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			LongWritable size = new LongWritable(value.getLength());
			context.write(key, size);
			
		}
		
		
		
	}
	
	
	public static class Wreducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		private IntWritable result = new IntWritable();
		private LongWritable rs = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			
			for (LongWritable intWritable : values) {
				sum+=intWritable.get();
			}
			rs.set(sum);
			context.write(key, rs);
		}

		
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
//		Path output = new Path("hdfs://master:9000/Rubic/62/output/");
//		if(fs.exists(output)){
//			fs.deleteOnExit(output);
//		}
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
//		job.setMapperClass(Wmap.class);
//		job.setReducerClass(Wreducer.class);
//		
//		job.setOutputKeyClass(Text.class);
//	    job.setOutputValueClass(LongWritable.class);
//		
//		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/upload/myData.txt"));
//		FileOutputFormat.setOutputPath(job, output);
//		job.waitForCompletion(true);
		
		job.setMapperClass(FileReadMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://master1:9000/Rubic/62/20news-bydate-train-seqfiles"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master1:9000/Rubic/62/20news-bydate-train-seq-words"));
		job.waitForCompletion(true);
		
	}

}


/*class Wmap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Log Log = LogFactory.getLog(Wmap.class);
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] strs = line.split(",");
		for (String string : strs) {
			word.set(string);
			context.write(word, one);
		}
	}
	
}*/
