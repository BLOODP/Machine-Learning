package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.httpclient.params.HttpParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.lda.LDAPrintTopics;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.SequenceFileDumper;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.vectors.VectorDumper;


import cn.edu.cqupt.rubic_framework.algorithm_interface.ErrorInputFormatException;
import cn.edu.cqupt.rubic_framework.algorithm_interface.OperationalDataOnHadoop;

public class VectorDemo /*implements OperationalDataOnHadoop*/{
	
	private String dataSource;
	private String subPath;
	private String platform;
	private Configuration conf;
	private FileSystem fs;
	protected List<NamedVector> nvs = new ArrayList<NamedVector>();
	protected String seqFileDir;
	protected String clusterDir;
	
	public void init(String dataSource,String subPath,Object configuration) throws IOException{
		this.dataSource = dataSource;//源数据位置
		this.subPath = subPath;//工作目录，所有结果文件都放在该目录下
		conf = (Configuration) configuration;
		fs = FileSystem.get(conf);
		
		int index = dataSource.indexOf(":");
		if(index==-1||dataSource.substring(0, index).equalsIgnoreCase("hdfs")){
			platform = "hadoop";
		}else{
			platform = "java";
		}
		
		this.seqFileDir = subPath+"seqfile/";
		this.clusterDir = subPath+"cluster/";
		
	}
	
	
	public String run(String dataSource, String subPath, Object configuration, double... arg3)
			throws ErrorInputFormatException {
		
			
			try {
				
				init(dataSource,subPath,configuration);
				
				transformToVector();
				
				writeSequenceFile();
				
				initCluster();
				
				
				Path resultPath = new Path(subPath);
				KMeansDriver.run(conf, new Path(seqFileDir), new Path(clusterDir), resultPath, 0.001, 10, true, 0, false);
			
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
			
		
		
		return subPath;
	}
	
	public void transformToVector() throws IOException{
		
		InputStreamReader inr = null;
		BufferedReader br = null;
		
		if("java".equalsIgnoreCase(platform)){
			File dataSet = new File(dataSource);
			
			FileInputStream in = new FileInputStream(dataSet);
			inr = new InputStreamReader(in);
			br = new BufferedReader(inr);
		}else{
			Path dataSet = new Path(dataSource);
			FSDataInputStream in = fs.open(dataSet);
			inr = new InputStreamReader(in);
			br = new BufferedReader(inr);
		}
		
		
		
		NamedVector nv;
		String line = br.readLine();
		while(line!=null&&line!=""&&line.length()>0){
			String[] strs = line.split(",");
			List<String> ls = new ArrayList<String>();
			ls.addAll(Arrays.asList(strs));
			String label = ls.get(4);
			ls.remove(4);
			double[] dl = new double[ls.size()];
			int i=0;
			for (String string : ls) {
				Double d = Double.valueOf(string);
				dl[i] = d;
				i++;
			}
			nv = new NamedVector(new DenseVector(dl), label);
			nvs.add(nv);
			line = br.readLine();
		}
		
		
		
	}
	
	
	public Path writeSequenceFile() throws IOException {
		
		Path seqFilePath = new Path(seqFileDir+"part-s-00000");
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, seqFilePath, Text.class, VectorWritable.class);
		
		VectorWritable vec = new VectorWritable();
		for(NamedVector vector:nvs){
			vec.set(vector);
			writer.append(new Text(vector.getName()), vec);
		}
		writer.close();
		System.out.println("ok");
		
		return seqFilePath;
	}
	
	
	public Path initCluster() throws IOException{
		
		Path clusterPath = new Path(clusterDir+"part-c-00000");
		
		DistanceMeasure measure = new EuclideanDistanceMeasure();//距离测度
		
		
//		clusterPath = RandomSeedGenerator.buildRandom(conf, path, clusterPath, 10, measure);
		
		SequenceFile.Writer Cwriter = new SequenceFile.Writer(fs, conf, clusterPath,
				Text.class, Kluster.class);
		
		for(int i=0;i<5;i++){
			NamedVector vector = nvs.get(i);
			Kluster cluster = new Kluster(vector, i, measure);
			Cwriter.append(new Text(cluster.getIdentifier()), cluster);
		}
		Cwriter.close();
		
		return clusterPath;
		
	}
	

	/*public void runOnMahout(){
		
		try {
			
			Path resultPath = new Path(subPath);
			KMeansDriver.run(conf, new Path(seqFileDir), new Path(clusterDir), resultPath, 0.001, 10, true, 0, false);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/
	
	
	public static void main(String[] args) throws Exception {
		
		
		/*File wine = new File("D:/ADT/iris.data");
		
		FileInputStream in = new FileInputStream(wine);
		InputStreamReader inr = new InputStreamReader(in);
		BufferedReader br = new BufferedReader(inr);
		
		List<NamedVector> nvs = new ArrayList<NamedVector>();
		
		NamedVector nv;
		String line = br.readLine();
		while(line!=null&&line!=""&&line.length()>0){
			String[] strs = line.split(",");
			List<String> ls = new ArrayList<String>();
			ls.addAll(Arrays.asList(strs));
			String label = ls.get(4);
			ls.remove(4);
			double[] dl = new double[ls.size()];
			int i=0;
			for (String string : ls) {
				Double d = Double.valueOf(string);
				dl[i] = d;
				i++;
			}
			nv = new NamedVector(new DenseVector(dl), label);
			nvs.add(nv);
			line = br.readLine();
		}*/
		
		
		
		/*Path path = new Path("/uesr/hadoop/kmeansIris/points/iris");
		Configuration conf = new Configuration();
		FileSystem fs = path.getFileSystem(conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, VectorWritable.class);
		
		VectorWritable vec = new VectorWritable();
		for(NamedVector vector:nvs){
			vec.set(vector);
			writer.append(new Text(vector.getName()), vec);
		}
		writer.close();
		System.out.println("ok");*/
		
		
		
		
		
		
		/*Path clusterPath = new Path("/uesr/hadoop/kmeansIris/clusters/part-00000");
		
		DistanceMeasure measure = new EuclideanDistanceMeasure();
		
		
//		clusterPath = RandomSeedGenerator.buildRandom(conf, path, clusterPath, 10, measure);
		
		SequenceFile.Writer Cwriter = new SequenceFile.Writer(fs, conf, clusterPath,
				Text.class, Kluster.class);
		
		for(int i=0;i<5;i++){
			NamedVector vector = nvs.get(i);
			Kluster cluster = new Kluster(vector, i, measure);
			Cwriter.append(new Text(cluster.getIdentifier()), cluster);
		}
		Cwriter.close();*/
		
		Configuration conf = new Configuration();
//		FileSystem fs = HDFSConnection.getFileSystem();
//		
//		
//		List<NamedVector> nvs = prepareDateMahuout("D:/ADT/iris.data","java",fs);
//		System.out.println(nvs.toString());
//		
//		
//		String seqFileDir = "/uesr/hadoop/kmeansIris/points/iris";
//		Path seqFilePath = writeSequenceFile(seqFileDir,nvs);
//		
//		
//		String clusterDir = "/uesr/hadoop/kmeansIris/clusters/part-00000";
//		Path clusterPath = initCluster(clusterDir,nvs,fs,conf);
		
		
		/*try {
			
			KMeansDriver.run(conf, new Path("/Rubic/62/mahout/2015_12_7_20.26/kmens+iris/seqfile"), new Path("/Rubic/62/canopytest/synthetic_control/test11/clusters-0-final"), new Path("/Rubic/62/canopytest/synthetic_control/test11/cluster"), 0.001, 10, true, 0, false);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
		
//		runOnMahout(conf);
		
		
//		SequenceFileDumper seqDumper = new SequenceFileDumper();
//		KMeansDriver.run(conf, path, clusterPath, new Path("testdata/output"), convergenceDelta, maxIterations, runClustering, clusterClassificationThreshold, runSequential)
	
		String[] args1 = {"-i","/Rubic/62/canopytest/synthetic_control/test11/cluster/clusters-*-final","-p","/Rubic/62/canopytest/synthetic_control/test11/cluster/clusteredPoints","-o","D:/canopy+kmeans.txt","-of"};
//		seqDumper.run(args1);
//		VectorDumper.main(args1);
//		ClusterDumper clusterDumper = new ClusterDumper(new Path("/Rubic/62/mahout/2015_12_3_16.18/kmens+iris/","clusters-*-final"), new Path("/Rubic/62/mahout/2015_12_3_16.18/kmens+iris/","clusteredPoints"));
		ClusterDumper clusterDumper = new ClusterDumper();
		try {
//			clusterDumper.printClusters(new String[]{"D:/mahoutkmeans.txt"});
			clusterDumper.run(args1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		
		/*SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("kmeansdemo/output/"+Cluster.CLUSTERED_POINTS_DIR+"/part-m-00000"), conf);

		IntWritable key = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
		while(reader.next(key, value)){
			System.out.println(value.toString()+"belongs to cluster "+key.toString());
		}*/
		/*
		Configuration configuration = new Configuration();
		VectorDemo v = new VectorDemo();
		try {
			v.run("D:/ADT/iris.data", "/Rubic/62/mahout/2015_11_18_2222/kmens+iris/", configuration, 0);
		} catch (ErrorInputFormatException e) {
			e.printStackTrace();
		}*/
	
	}

	
	
	
	

}
