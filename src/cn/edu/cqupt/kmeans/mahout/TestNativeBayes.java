package cn.edu.cqupt.kmeans.mahout;

import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.test.BayesTestMapper;
import org.apache.mahout.classifier.naivebayes.test.TestNaiveBayesDriver;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TestNativeBayes extends AbstractJob{

	private static final Pattern SLASH = Pattern.compile("/");
	
	public static void main(String[] args) throws Exception {
//		TestNativeBayes t = new TestNativeBayes();
//	  
//		t.run(new String[]{});
//		ToolRunner.run(new Configuration(), new TestNativeBayes(), args);
		
		Configuration conf = new Configuration();
		
		Path inputpath = new Path("hdfs://master:9000/tmp/maout-work-hadoop/20news-test-vectors/part-r-00000");
//		FileSystem fs = inputpath.getFileSystem(conf);
		
		FileSystem fs =FileSystem.get(inputpath.toUri(), conf);
		
//		Reader reader = new SequenceFile.Reader(fs, inputpath, conf);
//		
//		Text key = new Text();
//	      VectorWritable vw = new VectorWritable();
//	      while (reader.next(key, vw)) {
//	    	  
//	    	  System.out.println(key.toString()+"     "+vw.get().toString());
//	    	  
//	      }
//	      reader.close();
	     /* Pattern SLASH = Pattern.compile("/");
	      String s =SLASH.split("/talk.religion.misc/84568")[1];
	      System.out.println(s);*/
	      
	      Path output = new Path("/tmp/news");
	      
	      SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, output, Text.class, Text.class);
	      
	      Text key = new Text("/talk.religion.misc/84568");
	      Text value = new Text("Anyone interested in this mail email me or follow up this");
	      writer.append(key, value);
	      writer.close();
	      System.out.println("ok");
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		setConf(conf);
//		HadoopUtil.delete(getConf(), new Path("hdfs://master:9000/tmp/maout-work-hadoop/20news-testing-test"));
		HadoopUtil.delete(conf, new Path("hdfs://master:9000/tmp/maout-work-hadoop/20news-testing-test"));
	    Path model = new Path("hdfs://master:9000/tmp/maout-work-hadoop/model");
//	    HadoopUtil.cacheFiles(model, conf);
//	    DistributedCache.setCacheFiles(new URI[]{model.toUri()}, getConf());
	    DistributedCache.setCacheFiles(new URI[]{model.toUri()}, conf);
	    //the output key is the expected value, the output value are the scores for all the labels
	    Job testJob = prepareJob(new Path("hdfs://master:9000/tmp/maout-work-hadoop/20news-test-vectors"), new Path("hdfs://master:9000/tmp/maout-work-hadoop/20news-testing-test"), SequenceFileInputFormat.class, BayesTestMapper.class,
	            Text.class, VectorWritable.class, SequenceFileOutputFormat.class);
	    //testJob.getConfiguration().set(LABEL_KEY, getOption("--labels"));
	    
	    //boolean complementary = parsedArgs.containsKey("testComplementary"); //always result to false as key in hash map is "--testComplementary"
	    boolean complementary = true; //or  complementary = parsedArgs.containsKey("--testComplementary");
	    testJob.getConfiguration().set("class", String.valueOf(complementary));
	    testJob.waitForCompletion(true);
	    
	    
	    Map<Integer, String> labelMap = BayesUtils.readLabelIndex(conf, new Path("hdfs://master:9000/tmp/maout-work-hadoop/labelindex"));
	    
	    SequenceFileDirIterable<Text, VectorWritable> dirIterable =
	            new SequenceFileDirIterable<Text, VectorWritable>(new Path("hdfs://master:9000/tmp/maout-work-hadoop/20news-testing-test"),
	                                                              PathType.LIST,
	                                                              PathFilters.partFilter(),
	                                                              conf);
	    
	    ResultAnalyzer analyzer = new ResultAnalyzer(labelMap.values(), "DEFAULT");
	    analyzeResults(labelMap, dirIterable, analyzer);
	    System.out.println(analyzer);
	  
		return 0;
	}

	private void analyzeResults(Map<Integer, String> labelMap,
			SequenceFileDirIterable<Text, VectorWritable> dirIterable,
			ResultAnalyzer analyzer) {

	    for (Pair<Text, VectorWritable> pair : dirIterable) {
	      int bestIdx = Integer.MIN_VALUE;
	      double bestScore = Long.MIN_VALUE;
	      for (Vector.Element element : pair.getSecond().get().all()) {
	        if (element.get() > bestScore) {
	          bestScore = element.get();
	          bestIdx = element.index();
	        }
	      }
	      if (bestIdx != Integer.MIN_VALUE) {
	        ClassifierResult classifierResult = new ClassifierResult(labelMap.get(bestIdx), bestScore);
	        analyzer.addInstance(pair.getFirst().toString(), classifierResult);
	      }
	    }
	  
		
	}
	
	
}
