package cn.edu.cqupt.kmeans.util;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.mahout.text.WholeFileRecordReader;

public class MyMultipleTextInputFormat extends CombineFileInputFormat<IntWritable, BytesWritable>{

	@Override
	public RecordReader<IntWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		// TODO Auto-generated method stub
		return new CombineFileRecordReader<IntWritable, BytesWritable>((CombineFileSplit) split,
				context, WholeFileRecordReader.class);
	}

}
