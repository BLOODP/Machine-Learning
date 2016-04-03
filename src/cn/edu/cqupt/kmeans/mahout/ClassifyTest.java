package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.TrainLogistic;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;

public class ClassifyTest {
	
	public static void main(String[] args) throws IOException {
		
		LogisticModelParameters lmp = LogisticModelParameters.loadFrom(new File("D:/model"));
		
		CsvRecordFactory csv = lmp.getCsvRecordFactory();
		List<String> targets = csv.getTargetCategories();
		System.out.println(targets.toString());
//		Iterable<String> ps = csv.getPredictors();
//		for (String string : ps) {
//			
//			System.out.println(string);
//		}
		
//		Map<String, Set<Integer>> entries = csv.getTraceDictionary();
//		Set<Entry<String, Set<Integer>>> e = entries.entrySet();
//		for (Entry<String, Set<Integer>> entry : e) {
//			System.out.println(entry.getKey());
//		}
//		
		
		for (String key : csv.getTraceDictionary().keySet()){
			System.out.println("key   "+key);
		}
	    OnlineLogisticRegression lr = lmp.createRegression();
	    
//	    BufferedReader in = new BufferedReader(new FileReader("D:/donut-test.csv"));
//	    String line = in.readLine();
//	    System.out.println(line);
//	    csv.firstLine("\"x\",\"y\",\"shape\",\"color\",\"xx\",\"xy\",\"yy\",\"c\",\"a\",\"b\"");
//	    
//	    Iterable<String> ps = csv.getPredictors();
//		for (String string : ps) {
//			
//			System.out.println(string);
//		}
//	    line = in.readLine();
	    
	    Vector v = new SequentialAccessSparseVector(lmp.getNumFeatures());
	    
	    String l = "0.802415437065065,0.0978854028508067,21,2,0.643870533640319,0.07854475831082,0.00958155209126472,0.503141377562721,0.808363832523192,0.220502180491382";
	    FeatureVectorEncoder x = new ContinuousValueEncoder("x");
	    x.addToVector("0.802415437065065", v);
	    FeatureVectorEncoder y = new ContinuousValueEncoder("y");
	    y.addToVector("0.0978854028508067", v);
	    FeatureVectorEncoder c = new ContinuousValueEncoder("c");
	    c.addToVector("0.503141377562721", v);
	    FeatureVectorEncoder a = new ContinuousValueEncoder("a");
	    a.addToVector("0.808363832523192", v);
	    FeatureVectorEncoder Intercept_Term = new ConstantValueEncoder("Intercept Term");
	    Intercept_Term.addToVector(""+null, v);
	    double score = lr.classifyScalar(v);
	    System.out.println("score   "+score);
        int predictedClass = score > 0.5 ? 1 : 0;
        System.out.println(predictedClass);
//        String s = csv.getTargetLabel(target);
        String str1 = csv.getTargetLabel(predictedClass);
        System.out.println("    "+l+"    "+str1);
	    /*while (line != null) {
	        Vector v = new SequentialAccessSparseVector(lmp.getNumFeatures());
	        int target = csv.processLine(line, v);

	        double score = lr.classifyScalar(v);
	        int predictedClass = score > 0.5 ? 1 : 0;
	        String s = csv.getTargetLabel(target);
	        String x = csv.getTargetLabel(predictedClass);
	        System.out.println(target+"    "+line+"    "+s+"  "+x);
	        line = in.readLine();
	      }*/
	    
		
	}

}
