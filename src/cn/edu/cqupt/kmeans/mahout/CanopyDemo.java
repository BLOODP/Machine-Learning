package cn.edu.cqupt.kmeans.mahout;

import java.io.IOException;

import com.google.*;
import com.google.common.collect.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.syntheticcontrol.canopy.Job;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class CanopyDemo {
	
	
	public static void main(String[] args) throws Exception {
		
//		Path input = new Path("/user/hadoop/testdata/synthetic_control.data");
//		Path output = new Path("/Rubic/62/canopytest/synthetic_control/test14/data");
//		InputDriver.runJob(input, output,"org.apache.mahout.math.RandomAccessSparseVector");
		String o = "/Rubic/62/canopytest/synthetic_control/test11";
		Path output = new Path(o);
		CanopyDriver.run(new Configuration(), new Path("/Rubic/62/mahout/2015_12_7_20.26/kmens+iris/seqfile"), output, new EuclideanDistanceMeasure(), 1.8, 1.2, true, 0.0, false);
//		ClusterDumper clusterDumper = new ClusterDumper(new Path("/Rubic/62/canopytest/synthetic_control/test14/clusters-0-final"), new Path("/Rubic/62/canopytest/synthetic_control/test14/clusteredPoints"));
//		clusterDumper.printClusters(null);
		
//		Job.main(new String[]{});
//		DictionaryVectorizer.createTermFrequencyVectors(input, output, tfVectorsFolderName, baseConf, minSupport, maxNGramSize, minLLRValue, normPower, logNormalize, numReducers, chunkSizeInMegabytes, sequentialAccess, namedVectors)
//		TFIDFConverter.processTfIdf(input, output, baseConf, datasetFeatures, minDf, maxDF, normPower, logNormalize, sequentialAccessOutput, namedVector, numReducers)
		String[] args1 = {"-i",o+"/clusters-0-final","-p",o+"/clusteredPoints","-o","D:/canopy.txt"};
		ClusterDumper Dumper = new ClusterDumper();
		Dumper.run(args1);
		
	}

}
