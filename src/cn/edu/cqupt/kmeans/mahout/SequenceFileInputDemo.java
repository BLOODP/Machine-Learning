package cn.edu.cqupt.kmeans.mahout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.document.SequenceFileTokenizerMapper;

public class SequenceFileInputDemo {
	
	private static class SMapper extends Mapper<Text, Text, Text, Text>{

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString()+"    "+value.toString());
			super.map(key, value, context);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("DocumentProcessor::DocumentTokenizer: input-folder: ");
		job.setJarByClass(SequenceFileInputDemo.class);
		    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path input = new Path("/tmp/news");
		Path output = new Path("/tmp/news-o");
		HadoopUtil.delete(conf, output);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		    
		job.setMapperClass(SMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(0);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		 boolean succeeded = job.waitForCompletion(true);
		    if (!succeeded) {
		      throw new IllegalStateException("Job failed!");
		    }
	}

}
