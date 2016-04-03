package cn.edu.cqupt.kmeans.mahout;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.map.OpenObjectLongHashMap;
import org.apache.mahout.vectorizer.TFIDF;

import com.google.common.io.Closeables;

public class SparseVectorTest {

	
	public static void main(String[] args) throws IOException {
		
		OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<String>();
		
		Configuration conf = new Configuration();
	    Path dictionaryFile = new Path("/tmp/maout-work-hadoop/20news-vectors-dbg/dictionary.file-0");
	    // key is word value is id
	    for (Pair<Writable, IntWritable> record
	            : new SequenceFileIterable<Writable, IntWritable>(dictionaryFile, true, conf)) {
	    	dictionary.put(record.getFirst().toString(), record.getSecond().get());
	    	
	    }
	    
		
		long featureCount = 0;
	      long vectorCount = Long.MAX_VALUE;
	      Path filesPattern1 = new Path("/tmp/maout-work-hadoop/20news-vectors/df-count/part-r-00000");
	      for (Pair<IntWritable,LongWritable> record
	           : new SequenceFileDirIterable<IntWritable,LongWritable>(filesPattern1,
	                                                                   PathType.GLOB,
	                                                                   null,
	                                                                   null,
	                                                                   true,
	                                                                   conf)) {


	        IntWritable key = record.getFirst();
	        LongWritable value = record.getSecond();
	        if (key.get() >= 0) {
//	          freqWriter.append(key, value);
	        } else if (key.get() == -1) {
	          vectorCount = value.get();
	        }
	        featureCount = Math.max(key.get(), featureCount);

	      }
	    featureCount++;
	    System.out.println(featureCount+"    "+vectorCount);
		
		//解析file
//		File file = new File("D:\\hadoop\\20news-bydate-test\\comp.graphics\\38769");
	    File file = new File("D:\\38758");
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		TokenStream stream = analyzer.tokenStream("body", new FileReader(file));
	    CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
	    stream.reset();
	    StringTuple document = new StringTuple();
	    while (stream.incrementToken()) {
	      if (termAtt.length() > 0) {
	        document.add(new String(termAtt.buffer(), 0, termAtt.length()));
	      }
	    }
	    stream.end();
	    Closeables.close(stream, true);
	    System.out.println(document.toString());
	    
	   /* OpenObjectLongHashMap<String> wordCount = new OpenObjectLongHashMap<String>();
	    for (String word : document.getEntries()) {
	      if (wordCount.containsKey(word)) {
	        wordCount.put(word, wordCount.get(word) + 1);
	      } else {
	        wordCount.put(word, 1);
	      }
	    }*/
	    
	    
//	    System.out.println(wordCount.toString());
	    
	   /* Map<String, Integer> m = new HashMap<>();
	    int i = 0;
	    for (String string : wordCount.keys()) {
			System.out.println(string+"    "+wordCount.get(string));
			m.put(string, i++);
		}
	    System.out.println(m.toString());*/
	    
	    
	    //转换为NameVector
	    Vector vector = new RandomAccessSparseVector(1000, document.length());
	    for (String term : document.getEntries()) {
	        if (!term.isEmpty() && dictionary.containsKey(term)) { // unigram
	          int termId = dictionary.get(term);
	          vector.setQuick(termId, vector.getQuick(termId) + 1);
	        }
	      }
	    vector = new NamedVector(vector, "body");
	    System.out.println(vector.toString());
	    
	   /* Map<Integer, Long> dfcount = new HashMap<>();
	    for (Vector.Element e : vector.nonZeroes()) {
	        
	       System.out.println(e.index());
	      }
	    */
	    OpenIntLongHashMap dictionary1 = new OpenIntLongHashMap();
	    Path dictionaryFile1 = new Path("/tmp/maout-work-hadoop/20news-vectors/frequency.file-0");
	    // key is feature, value is the document frequency
	    for (Pair<IntWritable,LongWritable> record 
	         : new SequenceFileIterable<IntWritable,LongWritable>(dictionaryFile1, true, conf)) {
	    	dictionary1.put(record.getFirst().get(), record.getSecond().get());
	    }
	    TFIDF tfidf = new TFIDF();
	    
	    for (Vector.Element e : vector.nonZeroes()) {
	        if (!dictionary1.containsKey(e.index())) {
	          continue;
	        }
	        long df = dictionary1.get(e.index());
//	        if (maxDf > -1 && (100.0 * df) / vectorCount > maxDf) {
//	          continue;
//	        }
//	        if (df < minDf) {
//	          df = minDf;
//	        }
	        vector.setQuick(e.index(), tfidf.calculate((int) e.get(), (int) df, (int) featureCount, (int) vectorCount));
	      }
	    
	    System.out.println(vector.toString());
	    
	    
		Path filesPattern = new Path("/tmp/maout-work-hadoop/20news-vectors-dbg/wordcount/part-r-00000");
//	    for (Pair<Writable,Writable> record
//	            : new SequenceFileDirIterable<Writable,Writable>(filesPattern, PathType.GLOB, null, null, true, conf)) {
//	         
//
//	         Writable key = record.getFirst();
//	         System.out.println(record.toString());
//	       }
	    
		FileSystem fs = FileSystem.get(conf);
	    NaiveBayesModel model = NaiveBayesModel.materialize(new Path("hdfs://master:9000/tmp/maout-work-hadoop/model"), conf);
	    AbstractNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);
	    Vector v = classifier.classifyFull(vector);
	    System.out.println(v.toString());
	    
	    Map<Integer, String> labelMap = BayesUtils.readLabelIndex(conf, new Path("hdfs://master:9000/tmp/maout-work-hadoop/labelindex"));
	    int bestIdx = Integer.MIN_VALUE;
	      double bestScore = Long.MIN_VALUE;
	      for (Vector.Element element : v.all()) {
	        if (element.get() > bestScore) {
	          bestScore = element.get();
	          bestIdx = element.index();
	        }
	      }
	      
	      
	    System.out.println(bestScore+"    "+bestIdx);
	    
	    String result = labelMap.get(bestIdx);
	    System.out.println(result);
	    
	}
	
}
