package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.ConfusionMatrix;
import org.apache.mahout.classifier.sgd.AdaptiveLogisticRegression;
import org.apache.mahout.classifier.sgd.CrossFoldLearner;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.classifier.sgd.RunLogistic;
import org.apache.mahout.classifier.sgd.SGDHelper;
import org.apache.mahout.classifier.sgd.TrainLogistic;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.InteractionValueEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.ep.State;

import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;

public class LogisticDemo {
	
	private static final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
	private static final FeatureVectorEncoder encoder = new StaticWordValueEncoder("body");
	private static final FeatureVectorEncoder bias = new ConstantValueEncoder("Intercept");
	
	
	
	public static void main(String[] args) throws Exception {
		
//		String[] arg = {"--input","D:/donut.csv","--output","D:/model"
//				,"--target","color","--categories","2",
//				"--predictors","x","y","a","c","--types","numeric",
//				"--features","20","--passes","100","--rate","50"};
//		
//		TrainLogistic.main(arg);
//		
//		String[] arg1 = {"--input","D:/donut-test.csv","--model","D:/model","--auc","--confusion","--scores"};
//		
//		RunLogistic.main(arg1);
		
//		Dictionary dic = new Dictionary();
//		dic.intern("");
//		
		
//		System.out.println(Math.sqrt(9));
		
		/*BufferedReader br = new BufferedReader(new FileReader(""));
		List<String> symbols = new ArrayList<String>();
		String line = br.readLine();
		while(line != null){
			String[] pieces = line.split(",");
			
		}*/
		
		Multiset<String> overallCounts = HashMultiset.create();
		
		File base = new File("D:\\hadoop\\20news-bydate-train");
		int leakType = 0;
		
		Dictionary newsGroups = new Dictionary();
		encoder.setProbes(2);
		
		AdaptiveLogisticRegression ar = new AdaptiveLogisticRegression(20, 10000, new L1());
		ar.setInterval(1000);
		ar.setAveragingWindow(500);
		
		List<File> files = Lists.newArrayList();
		File[] directories = base.listFiles();
		Arrays.sort(directories, Ordering.usingToString());
		
		for (File file : directories) {
			if(file.isDirectory()){
				newsGroups.intern(file.getName());
				files.addAll(Arrays.asList(file.listFiles()));
			}
		}
		
		Collections.shuffle(files);
		System.out.println(files.size() + " training files");
		
		int k = 0;
		for (File file : files) {
			Multiset<String> words = ConcurrentHashMultiset.create();
			String ng = file.getParentFile().getName();
			int actual = newsGroups.intern(ng);
			Vector v = new RandomAccessSparseVector(10000);
			TokenStream ts = analyzer.tokenStream("body", new FileReader(file));
			CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
			ts.reset();
			while(ts.incrementToken()){
//				char[] termbuffer = termAtt.buffer();
//				String w = new String(termbuffer,1,termAtt.length());
				String w = ts.getAttribute(CharTermAttribute.class).toString();
				words.add(w);
//				encoder.addToVector(w, 1, v);
			}
			overallCounts.addAll(words);
			ts.end();
			ts.close();
			
		    bias.addToVector("", 1, v);
		    for (String word : words.elementSet()) {
		      encoder.addToVector(word, Math.log1p(words.count(word)), v);
		    }
		    
			ar.train(actual, v);
			k++;
			if(k % 100 ==0){
				System.out.println("tarin the "+k+" file");
			}
			State<AdaptiveLogisticRegression.Wrapper,CrossFoldLearner> best = ar.getBest();
			
			if(best!=null && k % 500 == 0){
				CrossFoldLearner model = best.getPayload().getLearner();
				double averageCorrect = model.percentCorrect();
				double averageLL = model.logLikelihood();
				System.out.printf("%d\t%.3f\t%.2f\t\n",k,averageLL,averageCorrect*100);
			}
			
			
		}
		
		
		ar.close();
		SGDHelper.dissect(0, newsGroups, ar, files, overallCounts);
		System.out.println("exiting main");
		
		
		ModelSerializer.writeBinary("/tmp/news-group.model",
	            ar.getBest().getPayload().getLearner().getModels().get(0));
		
	
		
		
		
		
		
		
		
	}

}
