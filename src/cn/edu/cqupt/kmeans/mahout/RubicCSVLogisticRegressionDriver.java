package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import cn.edu.cqupt.rubic_framework.algorithm_interface.ErrorInputFormatException;
import cn.edu.cqupt.rubic_framework.algorithm_interface.OperationalDataOnHadoop;
import cn.edu.cqupt.rubic_hadoop.config.HadoopConfiguration;

import com.google.common.io.Closeables;

public class RubicCSVLogisticRegressionDriver implements OperationalDataOnHadoop{
	
//	private static final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
//	private static final FeatureVectorEncoder column1 = new ContinuousValueEncoder("column1");
//	private static final FeatureVectorEncoder column2 = new ContinuousValueEncoder("column2");
	
	
	private static String inputFile;
	private static String outputFile;
	private static LogisticModelParameters lmp;
	private static int passes;
	private static boolean scores;
	private static OnlineLogisticRegression model;
	
//	private static String testInputFile;
	
	
	public static void main(String[] args) throws IOException {
		
		/*File base = new File("F:\\Rubic\\LRtestSet.txt");
		
		Dictionary catagory = new Dictionary();
		
//		AdaptiveLogisticRegression ar = new AdaptiveLogisticRegression(2, 10, new L1());
		OnlineLogisticRegression lr = new OnlineLogisticRegression(2, 100, new L1()).learningRate(50)
										.alpha(1 - 1.0e-3).lambda(1e-4);
		
		
//		List<FeatureVectorEncoder> encoder = new ArrayList<>();
		Map<Integer, FeatureVectorEncoder> encoder = new HashMap<>();
		encoder.put(0, column1);
		encoder.put(1, column2);
		
		for(int i =0;i<30;i++){
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(base)));
			
			String line = br.readLine();
			
			while(line!=null){
				
				Vector v = new RandomAccessSparseVector(100);
				String[] s = line.split("\t");
				String targetName = s[2];
				int actual = catagory.intern(targetName);
				for(int j=0;j<2;j++){
					
					encoder.get(j).addToVector(s[j], v);
//				System.out.print(s[i]);
				}
				
//				double logP = lr.logLikelihood(actual, v);
//				double p = lr.classifyScalar(v);
				
				lr.train(actual, v);
//			System.out.println();
				line = br.readLine();
//			System.out.println(v.toString());
			}
			br.close();
		}
		
//		lr.close();
		
//		ModelSerializer.writeBinary("D:/tmp/LRtestSet-model.model",
//	            ar.getBest().getPayload().getLearner().getModels().get(0));
//		System.out.println("ok");
		
		OutputStream modelOutput = new FileOutputStream("D:/tmp/LRtestSet-model.model");
		DataOutputStream out = new DataOutputStream(modelOutput);
//		out.writeInt(100);
//	    out.writeBoolean(false);
//	    out.writeInt(2);
//	    List<String> r = catagory.values();
//	    out.writeInt(r.size());
//	    for (String category : r) {
//	        out.writeUTF(category);
//	        System.out.println("category   :   "+category);
//	    }
	    lr.write(out);
		System.out.println("ok.....");
		
		
		FileInputStream in = new FileInputStream("D:/tmp/LRtestSet-model.model");
		DataInputStream modelin = new DataInputStream(in);
//		int numFeatures = modelin.readInt();
//		System.out.println(numFeatures);
//		boolean useBias = modelin.readBoolean();
//		System.out.println(useBias);
//		int maxTargetCategories = modelin.readInt();
//		System.out.println(maxTargetCategories);
//		int targetCategoriesSize = modelin.readInt();
//		List<String> targetCategories = Lists.newArrayListWithCapacity(targetCategoriesSize);
//		for (int i = 0; i < targetCategoriesSize; i++) {
//		      targetCategories.add(modelin.readUTF());
//		}
		lr.readFields(modelin);
		
		Vector v = new RandomAccessSparseVector(100);
		column1.addToVector("-0.733928", v);
		column2.addToVector("-1.618087", v);
		
		double score = lr.classifyScalar(v);
		System.out.println(score);
        int predictedClass = score > 0.5 ? 1 : 0;
        System.out.println(predictedClass);*/
		
		RubicCSVLogisticRegressionDriver driver = new RubicCSVLogisticRegressionDriver();
		
		try {
			driver.run(null);
		} catch (ErrorInputFormatException e) {
			e.printStackTrace();
		}
		
	}


	@Override
	public String run(HadoopConfiguration configuration)
			throws ErrorInputFormatException {
		
		inputFile = configuration.getDataSetPath();
		outputFile = configuration.getSubPath();
		Path inputPath = new Path(inputFile);
		
//		inputFile = "D:/donut.csv";
//		outputFile = "D:/rubicmodel";
		
		lmp = new LogisticModelParameters();
	    lmp.setTargetVariable("color");
	    lmp.setMaxTargetCategories(2);
	    lmp.setNumFeatures(20);
	    lmp.setUseBias(true);
	    
	    List<String> predictorList = new ArrayList<>();
	    predictorList.add("x");
	    predictorList.add("y");
	    predictorList.add("a");
	    predictorList.add("c");
	    
	    List<String> typeList = new ArrayList<>();
	    typeList.add("numeric");
	    typeList.add("numeric");
	    typeList.add("numeric");
	    typeList.add("numeric");
	    
	    lmp.setTypeMap(predictorList, typeList);

	    lmp.setLambda(1e-4);
	    lmp.setLearningRate(50);
	    
	    scores = false;
	    passes = 100;
		
	    CsvRecordFactory csv = lmp.getCsvRecordFactory();
	    OnlineLogisticRegression lr = lmp.createRegression();
	    
	    double logPEstimate = 0;
	    int samples = 0;
	    
	    Configuration conf = new Configuration();
	    FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			for (int pass = 0; pass < passes; pass++) {
				try {
					
					FSDataInputStream in = fs.open(inputPath);
					BufferedReader br = new BufferedReader(new InputStreamReader(in));
					
					csv.firstLine(br.readLine()); //读取csv文件的第一行，获取第一列中的变量名称
					
					String line = br.readLine();
					
					while (line != null) {
						/*     获取每一行的target以及predictors            */
						Vector input = new RandomAccessSparseVector(lmp.getNumFeatures());  
						int targetValue = csv.processLine(line, input);
						
						
						// check performance while this is still news
						double logP = lr.logLikelihood(targetValue, input);
						if (!Double.isInfinite(logP)) {
							if (samples < 20) {
								logPEstimate = (samples * logPEstimate + logP) / (samples + 1);
							} else {
								logPEstimate = 0.95 * logPEstimate + 0.05 * logP;
							}
							samples++;
						}
						double p = lr.classifyScalar(input);
						if (true) {
							System.out.printf(Locale.ENGLISH, "%10d %2d %10.2f %2.4f %10.4f %10.4f%n",
									samples, targetValue, lr.currentLearningRate(), p, logP, logPEstimate);
						}
						
						/*  训练并更新模型 */
						lr.train(targetValue, input);
						
						line = br.readLine();
					}
					br.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	    
	    
	    
	    /*    保存已训练好的模型    */
	    try {
//			OutputStream modelOutput = new FileOutputStream(outputFile);
	    	Path outputPath = new Path(outputFile+"/part-r-001");
	    	fs = outputPath.getFileSystem(conf);
	    	FSDataOutputStream modelOutput = fs.create(outputPath);
			lmp.saveTo(modelOutput);
			Closeables.close(modelOutput, false);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    model = lr;
	    try {
			runLogistic();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    System.out.println("outputFile  -->>>  "+outputFile);
		
		return outputFile;
	}
	
	public void runLogistic() throws IOException{
		
		Auc collector = new Auc();
		CsvRecordFactory csv = lmp.getCsvRecordFactory();
		File testFile = new File("D:/donut-test.csv");
		FileInputStream in;
		InputStreamReader inr;
		BufferedReader br;
		try {
			in = new FileInputStream(testFile);
			inr = new InputStreamReader(in);
			br = new BufferedReader(inr);
			
			String line = br.readLine();
			csv.firstLine(line);
			line = br.readLine();
			
			while(line!=null){
				Vector v = new SequentialAccessSparseVector(lmp.getNumFeatures());
		        int target = csv.processLine(line, v);
		        double score = model.classifyScalar(v);
		        collector.add(target, score);
				
				line = br.readLine();
			}
			
			System.out.println("AUC   ---   "+collector.auc());
			
			br.close();
			inr.close();
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	/*private void parseInputFile(String input) throws IOException {
		
		Path inputPath = new Path(input);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(inputPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		
		
	}*/
	
}
