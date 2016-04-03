package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.csv.CSVUtils;
import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.TextValueEncoder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 根据已训练好的模型，对输入的数据进行分类
 * @author Guqngqin He
 *
 */
public class RubicCSVLogisticsClassifyDriver {
	
	public static String modelFile;  //已训练好的模型
	public static String inputFile;  //待分类的数据
	
	private String targetVariable;
	private Map<String, String> typeMap;
	private int numFeatures;
	private boolean useBias;
	private int maxTargetCategories;
	private List<String> targetCategories;
	private CsvRecordFactory csv;
	private OnlineLogisticRegression lr;
	private Map<Integer, FeatureVectorEncoder> predictorEncoders = new HashMap<Integer, FeatureVectorEncoder>();
	
	private List<Integer> predictors = new ArrayList<Integer>();
	
	private static final Map<String, Class<? extends FeatureVectorEncoder>> TYPE_DICTIONARY =
	          ImmutableMap.<String, Class<? extends FeatureVectorEncoder>>builder()
	                  .put("continuous", ContinuousValueEncoder.class)
	                  .put("numeric", ContinuousValueEncoder.class)
	                  .put("n", ContinuousValueEncoder.class)
	                  .put("word", StaticWordValueEncoder.class)
	                  .put("w", StaticWordValueEncoder.class)
	                  .put("text", TextValueEncoder.class)
	                  .put("t", TextValueEncoder.class)
	                  .build();
	
	
	public static void main(String[] args) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		RubicCSVLogisticsClassifyDriver driver = new RubicCSVLogisticsClassifyDriver();
		driver.classify();
	}
	
	public void classify() throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		
		BufferedReader in = new BufferedReader(new FileReader("D:/donut-test.csv"));
		String line = in.readLine();
		String[] variableNames = CSVUtils.parseLine(line);
		final Map<String, Integer> vars = new HashMap<String, Integer>();
		for (int i = 0; i < variableNames.length; i++) {
			System.out.println(variableNames[i]);
			vars.put(variableNames[i], i);
		}
		
	    
	    InputStream input = new FileInputStream(new File("D:/model"));
	    DataInputStream dataInput = new DataInputStream(input);
	    readFields(dataInput);
	    
	    for (Entry<String, String> entry : typeMap.entrySet()) {
			String name = entry.getKey();
			System.out.println(name);
			Integer column = vars.get(name);
			predictors.add(column);
			Class<? extends FeatureVectorEncoder> c = TYPE_DICTIONARY.get(entry.getValue());
			Constructor<? extends FeatureVectorEncoder> constructor = c.getConstructor(String.class);
			FeatureVectorEncoder encoder = constructor.newInstance(name);
			predictorEncoders.put(column, encoder);
		}
	    
	    line = in.readLine();
	    while(line!=null){
	    	System.out.println(line);
	    	Vector v = new SequentialAccessSparseVector(numFeatures);
	    	String[] strs = CSVUtils.parseLine(line);
	    	
	    	/*  将每一行需要用于计算的特征转换为向量表示    */
	    	for (Entry<Integer, FeatureVectorEncoder> entry : predictorEncoders.entrySet()) {
	    		entry.getValue().addToVector(strs[entry.getKey()], v);
			}
	    	FeatureVectorEncoder Intercept_Term = new ConstantValueEncoder("Intercept Term");
		    Intercept_Term.addToVector(""+null, v);
		    
		    double score = lr.classifyScalar(v);  //计算分值
		    System.out.print(score+"  ");
		    int predictedClass = score > 0.5 ? 1 : 0;
		    String result = csv.getTargetLabel(predictedClass);  //得到分类的结果
		    System.out.println(result);
	    	line = in.readLine();
	    }
		in.close();
	}
	
	
	/**
	 * 读入模型中的数据，并创建OnlineLogisticRegression,CsvRecordFactory
	 * @param in  模型
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException{

	    targetVariable = in.readUTF();
	    int typeMapSize = in.readInt();
	    typeMap = Maps.newHashMapWithExpectedSize(typeMapSize);
	    for (int i = 0; i < typeMapSize; i++) {
	      String key = in.readUTF();
	      String value = in.readUTF();
	      typeMap.put(key, value);
	    }
	    numFeatures = in.readInt();
	    useBias = in.readBoolean();
	    maxTargetCategories = in.readInt();
	    int targetCategoriesSize = in.readInt();
	    targetCategories = Lists.newArrayListWithCapacity(targetCategoriesSize);
	    for (int i = 0; i < targetCategoriesSize; i++) {
	      targetCategories.add(in.readUTF());
	    }
	    double lambda = in.readDouble();
	    double learningRate = in.readDouble();
	    csv = new CsvRecordFactory(targetVariable, typeMap)
        .maxTargetValue(maxTargetCategories)
        .includeBiasTerm(useBias);
	    csv.defineTargetCategories(targetCategories);
	    lr = new OnlineLogisticRegression();
	    lr.readFields(in);
	  
	}

}
