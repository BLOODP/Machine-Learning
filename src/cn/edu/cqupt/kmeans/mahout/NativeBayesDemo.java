package cn.edu.cqupt.kmeans.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.mahout.classifier.naivebayes.test.TestNaiveBayesDriver;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.utils.SplitInput;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

public class NativeBayesDemo {
	
	public static void main(String[] args) throws Exception {
		
		/*String[] arg = {"-c", "UTF-8", "-i", "hdfs://master1:9000/Rubic/62/20news-bydate-train", "-o",
				 "hdfs://master1:9000/Rubic/62/20news-bydate-train-seqfiles","-ow"};
		
		SequenceFilesFromDirectory.main(arg);*/
		
		/*String[] arg1 = {"-i","hdfs://master1:9000/Rubic/62/20news-bydate-train-seqfiles","-o","hdfs://master1:9000/Rubic/62/20news-bydate-train-vectors","-ow","-lnorm","-nv","-wt","tfidf"};
		
		SparseVectorsFromSequenceFiles.main(arg1);*/
//		Path input = new Path("/tmp/maout-work-hadoop/20news-test-vectors/part-r-00000");
//		Configuration conf = new Configuration();
//		FileSystem fs = input.getFileSystem(conf);
//	      Reader reader = new Reader(fs,input, conf);
//	      Text key = new Text();
//	      VectorWritable vw = new VectorWritable();
//	      while (reader.next(key, vw)) {
////	        writer.append(new Text(SLASH.split(key.toString())[1]),
////	            new VectorWritable(classifier.classifyFull(vw.get())));
//	    	  System.out.println(key.toString()+ "      "+vw.get().toString());
//	      }
//	      reader.close();
		
		
//		SplitInput
	/*	String[] arg2 = {"-i","hdfs://master1:9000/Rubic/62/20news-bydate-train-vectors/tfidf-vectors","--trainingOutput","hdfs://master1:9000/Rubic/62/20news-bydate-training-vectors"
								,"--testOutput","hdfs://master1:9000/Rubic/62/20news-bydate-testing-vectors",
								"--randomSelectionPct","40",
								"--overwrite","--sequenceFiles","-xm","sequential"};
		SplitInput.main(arg2);
		*/
		
		String[] arg3 = {"-i","hdfs://master1:9000/Rubic/62/20news-bydate-training-vectors","-el",
							"-o","hdfs://master1:9000/Rubic/62/modeltest1",
							"-li","hdfs://master1:9000/Rubic/62/labelindextest1",
							"-ow","-c"};
		
		TrainNaiveBayesJob.main(arg3);
		/*
		String[] arg4 = {"-i","hdfs://master1:9000/Rubic/62/20news-bydate-testing-vectors",
							"-m","hdfs://master1:9000/Rubic/62/model",
							"-l","hdfs://master1:9000/Rubic/62/labelindex","-ow",
							"-o","hdfs://master1:9000/Rubic/62/20news-testing-seq","-c"};
		
		TestNaiveBayesDriver.main(arg4);*/
		
	}
	

}
