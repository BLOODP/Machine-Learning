package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.RunLogistic;

public class AUCTest {
	
	public static void main(String[] args) throws Exception {
		
		Auc x = new Auc();
		
		/*BufferedReader in = new BufferedReader(new FileReader(""));
		int lineCount = 0;
		String line = in.readLine();*/
		
		for (int i = 0; i < 100; i++) {
			x.add(0, 0.0401);
		}
		for (int i = 0; i < 10; i++) {
			x.add(1, 0.5365);
		}
//		for (int i = 0; i < 10; i++) {
//			x.add(0, 1.00);
//		}
		
//		x.add(0, 0.009);
//		x.add(0, 0.001);
//		x.add(1,0.984);
//		x.add(1,0.990);
//		x.add(0,0.001);
//		x.add(1,0.975);
//		x.add(1,0.811);
//		x.add(0,0.040);
//		x.add(0,0.049);
//		x.add(0,0.597);
//		x.add(0,0.171);
//		x.add(1,0.903);
//		x.add(1,0.274);
//		x.add(1,0.919);
//		x.add(1,0.998);
//		x.add(0,0.023);
//		x.add(1,0.990);
//		x.add(0,0.003);
//		x.add(1,0.960);
//		x.add(0,0.000);
		
		
		
		
		
		System.out.println(x.auc());
		
		/*String[] arg1 = {"--input","D:/donut-test.csv","--model","D:/model","--auc","--confusion","--scores"};
		RunLogistic.main(arg1);
		*/
		
	}

}
