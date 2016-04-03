package cn.edu.cqupt.kmeans.mahout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.NewsgroupHelper;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.Dictionary;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;

public class ClassifyNews {
	
	public static void main(String[] args) throws FileNotFoundException, IOException {


	    File base = new File("D:\\hadoop\\20news-bydate-train");
	    File testFile = new File("D:\\hadoop\\20news-bydate-test\\talk.religion.misc\\84511");
	    //contains the best model
	    OnlineLogisticRegression classifier =
	        ModelSerializer.readBinary(new FileInputStream("D:\\tmp\\news-group.model"), OnlineLogisticRegression.class);

	    Dictionary newsGroups = new Dictionary();
	    Multiset<String> overallCounts = HashMultiset.create();

//	    List<File> files = Lists.newArrayList();
	    for (File newsgroup : base.listFiles()) {
	      if (newsgroup.isDirectory()) {
	        newsGroups.intern(newsgroup.getName());
//	        files.addAll(Arrays.asList(newsgroup.listFiles()));
	      }
	    }
//	    System.out.println(files.size() + " test files");
//	    ResultAnalyzer ra = new ResultAnalyzer(newsGroups.values(), "DEFAULT");
	   /* for (File file : files) {
	      String ng = file.getParentFile().getName();

	      int actual = newsGroups.intern(ng);
	      NewsgroupHelper helper = new NewsgroupHelper();
	      //no leak type ensures this is a normal vector
	      Vector input = helper.encodeFeatureVector(file, actual, 0, overallCounts);
	      Vector result = classifier.classifyFull(input);
	      int cat = result.maxValueIndex();
	      double score = result.maxValue();
	      double ll = classifier.logLikelihood(actual, input);
	      ClassifierResult cr = new ClassifierResult(newsGroups.values().get(cat), score, ll);
	      ra.addInstance(newsGroups.values().get(actual), cr);
	      
	      String str1 = newsGroups.values().get(actual);
	      
	      String str2 = newsGroups.values().get(cat);
	      
	      String s3 = file.getAbsolutePath();
	      
	      System.out.println(str1+"    "+str2+"    "+s3);

	    }
	    System.out.println(ra);*/
	    
	    

//	      String ng = file.getParentFile().getName();

//	      int actual = newsGroups.intern(ng);
	      NewsgroupHelper helper = new NewsgroupHelper();
	      //no leak type ensures this is a normal vector
	      Vector input = helper.encodeFeatureVector(testFile, 0, 0, overallCounts);
	      Vector result = classifier.classifyFull(input);
	      int cat = result.maxValueIndex();
	      double score = result.maxValue();
//	      double ll = classifier.logLikelihood(actual, input);
//	      ClassifierResult cr = new ClassifierResult(newsGroups.values().get(cat), score, 0);
//	      ra.addInstance(newsGroups.values().get(actual), cr);
	      
//	      String str1 = newsGroups.values().get(actual);
	      
//	      classifier.
//	      result.
	      String str2 = newsGroups.values().get(cat);
//	      String str2 = ""+cat;
	      int i = classifier.numCategories();
	      String s3 = testFile.getAbsolutePath();
	      
	      System.out.println("    "+str2+"    "+s3+"  "+i);

	    
	  
	}

}
