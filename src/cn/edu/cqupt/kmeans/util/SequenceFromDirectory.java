package cn.edu.cqupt.kmeans.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.text.MultipleTextFileInputFormat;

public class SequenceFromDirectory {
	
	public static class SequenceFromDirectoryMapper extends Mapper<LongWritable, BytesWritable, Text, Text>{

		private Text fileValue = new Text();
		@Override
		protected void map(LongWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			System.out.println(key.toString());
//			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
//			String filename = filePath.getParent().toString()+"/"+filePath.getName();
//			Text text = new Text(filename);
			fileValue.set(value.getBytes(), 0, value.getBytes().length);
			Configuration configuration = context.getConfiguration();
		    Path filePath = ((CombineFileSplit) context.getInputSplit()).getPath((int) key.get());
		    String relativeFilePath = HadoopUtil.calcRelativeFilePath(configuration, filePath);
		    String filename = Path.SEPARATOR + relativeFilePath;
			context.write(new Text(filename), fileValue);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("baseinputpath", "hdfs://master1:9000/Rubic/62/20news-bydate-train/alt.atheism/");
		DistributedCache.addFileToClassPath(new Path("/input/jars/mahout-0.9/mahout-integration-0.9.jar"), conf, FileSystem.get(conf));
		Job job = new Job(conf, "SequenceFromfiletext");
		job.setJarByClass(SequenceFromDirectory.class);
		job.setMapperClass(SequenceFromDirectoryMapper.class);
		job.setNumReduceTasks(0);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(MultipleTextFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		long chunkSizeInBytes = 64 * 1024 * 1024;
		FileInputFormat.addInputPath(job, new Path("hdfs://master1:9000/Rubic/62/20news-bydate-train/alt.atheism/"));
		FileInputFormat.setMaxInputSplitSize(job, chunkSizeInBytes);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master1:9000/Rubic/62/20news-bydate-train-seqfiles-3"));
		FileOutputFormat.setCompressOutput(job, true);
		job.waitForCompletion(true);
		
//		int chunkSizeInMB = 64;
//		FileSystem fs = FileSystem.get(conf);
//		Path input = new Path("hdfs://master1:9000/Rubic/62/20news-bydate-train/");
//	    FileStatus fsFileStatus = fs.getFileStatus(input);
//		String inputDirList = HadoopUtil.buildDirList(fs, fsFileStatus);
//		conf.set("baseinputpath", input.toString());
//		long chunkSizeInBytes =  100* 1024;
//		
//		job.setMapperClass(SequenceFromDirectoryMapper.class);
//		job.setNumReduceTasks(0);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setInputFormatClass(MultipleTextFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		FileInputFormat.setInputPaths(job, inputDirList);
//		FileInputFormat.setMaxInputSplitSize(job, chunkSizeInBytes);
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://master1:9000/Rubic/62/20news-bydate-train-all-seq"));
		
	}

}
