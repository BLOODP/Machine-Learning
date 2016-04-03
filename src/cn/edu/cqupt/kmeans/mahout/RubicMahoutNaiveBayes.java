package cn.edu.cqupt.kmeans.mahout;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.utils.SplitInput;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

public class RubicMahoutNaiveBayes {
	
	private String dataSetInput;
	private String output;
	private String modelInput;
	private String labelIndex;
	
	public static void main(String[] args) {
		
	}
	
	public void trainNaiveBayes() throws Exception{
		
		
		/**
		 * 将文本文件转换为SequenceFile格式文件
		 */
		String inputDataSetDir = "";  //文件的输入目录
		String seqFilesOutputDir = "";  //SequenceFile文件输出目录
		String[] seqFromDirArgs = {"-c", "UTF-8", "-i", inputDataSetDir, "-o",
				seqFilesOutputDir,"-ow"};
		SequenceFilesFromDirectory.main(seqFromDirArgs);
		
		//将SequenceFile格式文件转换为向量文件
		String sparsedVectorsOutputDir = "";
		String[] sparseVectorsArgs = {"-i",seqFilesOutputDir,"-o",sparsedVectorsOutputDir,"-ow","-lnorm","-nv","-wt","tfidf"};
		SparseVectorsFromSequenceFiles.main(sparseVectorsArgs);
		
		
		//将数据集按照比例切分成训练数据集和测试数据集
		String trainingDatasOutput = "";
		String testDatasOutput = "";
		String[] splitsArgs = {"-i",sparsedVectorsOutputDir,"--trainingOutput",trainingDatasOutput
				,"--testOutput",testDatasOutput,
				"--randomSelectionPct","40",
				"--overwrite","--sequenceFiles","-xm","sequential"};
		SplitInput.main(splitsArgs);
		
		/**   开始训练         **/
		String bayesModelFile = "";
		String labelIndexFile = "";
		String[] trainNaiveBayesArgs = {"-i",trainingDatasOutput,"-el","-o",bayesModelFile,"-li",labelIndexFile,"-ow","-c"};
		TrainNaiveBayesJob.main(trainNaiveBayesArgs);
		
		
	}
	
	
	
	public Object[] run(NamedVector vector) throws IOException{
		
		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
		Path modelInputPath = new Path(modelInput);
		NaiveBayesModel model = NaiveBayesModel.materialize(modelInputPath, conf);
		AbstractNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);
		Vector v = classifier.classifyFull(vector);
		
		Path labelIndexPath = new Path(labelIndex);
		Map<Integer, String> labelMap = BayesUtils.readLabelIndex(conf, labelIndexPath);
		int bestIdx = Integer.MIN_VALUE;
		double bestScore = Long.MIN_VALUE;
		for (Vector.Element element : v.all()) {
	        if (element.get() > bestScore) {
	          bestScore = element.get();
	          bestIdx = element.index();
	        }
	      }
		
		String result = labelMap.get(bestIdx);
		String name = vector.getName();
		return new Object[]{name,bestScore,result};
		
		
	}
	

}
