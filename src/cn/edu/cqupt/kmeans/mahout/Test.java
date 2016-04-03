package cn.edu.cqupt.kmeans.mahout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.similarity.precompute.example.GroupLensDataModel;
import org.apache.mahout.classifier.ConfusionMatrix;
import org.apache.mahout.classifier.sgd.TestNewsGroups;
import org.apache.mahout.classifier.sgd.TrainNewsGroups;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;
import org.apache.mahout.vectorizer.DocumentProcessor;

import com.google.common.base.Optional;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

public class Test {
	
	public static void st(final String s){
		
		Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				System.out.println(s);
				
			}
		});
		t.start();
	}
	
	private static boolean ready;
	private static int number;
	
	private static class ReaderThread extends Thread {

		@Override
		public void run() {
			while(!ready)
				Thread.yield();
			System.out.println(number);
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
//		
//		new ReaderThread().start();
////		int i =0;
////		while(i<100){
////			i++;
////		}
//		number = 42;
//		ready = false;
		
		
		/*
		
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL("hdfs://master:9000/Rubic/62/dataset/iris.data");
//		String f = url.getFile();
		String f = url.getAuthority();
		System.out.println(f);
		File ff = new File(f);
		FileInputStream fin = new FileInputStream(ff);
		InputStreamReader insr = new InputStreamReader(fin);
		BufferedReader br = new BufferedReader(insr);
		String line = br.readLine();
		while(line!=null){
			System.out.println(line);
			line = br.readLine();
		}
		
		*/
		
		/*Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path dst = new Path("/uesr/hadoop/extracted");
		fs.mkdirs(dst);
		System.out.println("ok");
		Path src = new Path("D:/reuters-extracted");
		fs.copyFromLocalFile(src, dst);
		System.out.println("ok");*/
		
//		Path outGlob = new Path("/uesr/hadoop/extracted/reuters-kmeans-clusters", "clusters-*-final");
//	    Path clusteredPoints = new Path("/uesr/hadoop/extracted/reuters-kmeans-clusters","clusteredPoints");
//	    ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
//	    clusterDumper.printClusters(null);
		
//	    ClusterDumper.main(args)
//	    
//	    String[] args1 = {"-dt","sequencefile","-d","/uesr/hadoop/extracted/reuters-vectors/dictionary.file-*","-i","/uesr/hadoop/extracted/reuters-kmeans-clusters/clusters-17","-b","10","-n","10"};
//	    ClusterDumper.main(args1);
		
//		DisplayKMeans.main(args);
//		DisplayClustering.main(args);
		
//		Path samples = new Path("/uesr/hadoop/kmeansIris/points");
//		Path output = new Path("/uesr/hadoop/DisplayKMeans/output");
//		DistanceMeasure measure = new ManhattanDistanceMeasure();
//		
		
		
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		
		/*Path input = new Path("D:/apple.txt");//apple的数据文件存放在本地磁盘D盘
		Path data = new Path("/user/hadoop/testdata/apple.data");//将转换后的数据保存为/user/hadoop/testdata/apple.data
		Path output = new Path("/user/hadoop/testdata/apples");
		if(fs.exists(data)){
			fs.deleteOnExit(data);
		}
		if(fs.exists(output)){
			fs.deleteOnExit(output);
		}
//		fs.copyFromLocalFile(input, output);//将本地上的数据上传到HDFS上的'/user/hadoop/testdata/apples'
		InputDriver.runJob(output, data, "org.apache.mahout.math.DenseVector");
		
		
		System.out.println("ok");*/
		
		/*File wine = new File("D:/apple.txt");
		
		FileInputStream in = new FileInputStream(wine);
		InputStreamReader inr = new InputStreamReader(in);
		BufferedReader br = new BufferedReader(inr);
		
		List<NamedVector> nvs = new ArrayList<NamedVector>();
		NamedVector nv;
		String line = br.readLine();
		while(line!=null&&line!=""&&line.length()>0){
			String[] strs = line.split(",");//可根据实际情况指定分隔符
			List<String> ls = new ArrayList<String>();
			ls.addAll(Arrays.asList(strs));//将strs转化成List，方便后续操作
			String label = ls.get(3);
			ls.remove(3);
			double[] dl = new double[ls.size()];
			int i=0;
			for (String string : ls) {
				Double d = Double.valueOf(string);
				dl[i] = d;
				i++;
			}
			nv = new NamedVector(new DenseVector(dl), label);//将label及每个苹果的定义与向量相关联
			nvs.add(nv);
			line = br.readLine();
		}
		System.out.println(nvs.toString());//将经转换后的Vector打印
*/		
		/*Path data = new Path("/user/hadoop/testdata/apple.data");
		if(fs.exists(data)){
			fs.deleteOnExit(data);
		}
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, data, Text.class, VectorWritable.class);
		
		VectorWritable vec = new VectorWritable();
		for (NamedVector vector : nvs) {
			vec.set(vector);
			writer.append(new Text(vector.getName()), vec);
		}
		writer.close();
		System.out.println("ok");*/
		
		
		/*Path data = new Path("/user/hadoop/testdata/apple.data");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, data, conf);
		Text key = new Text();
		VectorWritable value = new VectorWritable();
		while(reader.next(key, value)){
			System.out.println(key.toString()+"   "+value.get().asFormatString());
		}
		reader.close();*/
		
//		Path path = new Path("/user/hadoop/testdata/apple/cluster/part-00000");
//		SequenceFile.Writer cwriter = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);
//		
//		for (int i = 0; i < 2; i++) {
//			Vector vec = nvs.get(i);
//			Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
//			cwriter.append(new Text(cluster.getIdentifier()), cluster);
//		}
//		cwriter.close();
//		
//		KMeansDriver.run(conf, new Path("/user/hadoop/testdata/apple.data"), new Path("/user/hadoop/testdata/apple/cluster"), new Path("/user/hadoop/testdata/apple/output"), 0.01, 10, true, 0.0, false);
		
//		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/user/hadoop/testdata/apple/output/"+Cluster.CLUSTERED_POINTS_DIR+"/part-m-00000"), conf);
//
//		IntWritable key = new IntWritable();
//		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
//		while(reader.next(key, value)){
//			System.out.println(value.toString()+"belongs to cluster "+key.toString());
//		}
		
		
//		Optional<Integer> passibe = Optional.of(5);
//		System.out.println(passibe.isPresent());
//		System.out.println(passibe.get());
		
//		Path src = new Path("/files");
//		Path dst = new Path("/files/merged");
//		if(fs.exists(dst)){
//			fs.deleteOnExit(dst);
//		}
//		
//		FileStatus[] status = fs.listStatus(src);
//		FSDataOutputStream out = fs.create(dst);
//		
//		for (FileStatus fileStatus : status) {
//			Path file = fileStatus.getPath();
//			System.out.println(file.getName());
//			FSDataInputStream in = fs.open(file);
//			IOUtils.copyBytes(in, out, 4096, false);
//			in.close();
//		}
//		out.close();
		
		/*int i =1;
		boolean flag = true;
		while(flag){
			i++;
			System.out.println(i);
//			if(i==100)
//				break;
			Thread.sleep(1000);
		}
		
		if(i==2){
			flag = false;
		}*/
		
/*
		System.out.println(Thread.currentThread().getName());
		for(int i=0; i<10; i++){
			new Thread("" + i){
					public void run(){
					System.out.println("Thread: " + getName() + "running");
					}
			}.start();
		}*/
		
		/*GroupLensDataModel.readResourceToTempFile("");
		
		DataModel dataModel=null;
		UserSimilarity similarity = new CachingUserSimilarity(new EuclideanDistanceSimilarity(dataModel), dataModel);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(10, 0.2,similarity, dataModel,0.2);
		Recommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
		Writer writer = new OutputStreamWriter(out, cs)
		new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(data))
		IRStatistics*/
		
		/*DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    
	    Option inputOpt = obuilder.withLongName("input").withRequired(false).withShortName("i")
	        .withArgument(abuilder.withName("input").withMinimum(1).withMaximum(1).create())
	        .withDescription("The Path for input data directory.").create();
	    
	    Option helpOpt = DefaultOptionCreator.helpOption();
	    
	    Group group = gbuilder.withName("Options").withOption(inputOpt).withOption(helpOpt).create();
	    
	    Parser parser = new Parser();
	    parser.setGroup(group);
	    CommandLine cmdLine = parser.parse(new String[]{"-i","D:/hadoop"});
		
	    String input = cmdLine.getValue(inputOpt).toString();
		System.out.println(input);*/
		
		/*String str = "Key: 2: Value: wt: 1.0 distance: 0.14694216549377498  vec: Iris-setosa = [5.100, 3.500, 1.400, 0.200]";
		int index = str.lastIndexOf(":");
		String s1 = str.substring(index+2);
		System.out.println(str.substring(index+2));
		String s2 = s1.replace("=", ":");
		System.out.println(s2);
		int index2 = s1.lastIndexOf("=");
		String s3 = s1.substring(0, index2-1);
		System.out.println(s3);
		
		String s4 = s1.substring(index2+3,s1.length()-1);
		System.out.println(s4);
		
		StringBuffer stf = new StringBuffer();
		stf.append(s4+", ");
		stf.append(s3);
		System.out.println(stf);
		
		int index3 = str.indexOf(":");
		String s5 = str.substring(index3+2, 6);
		System.out.println(s5);*/
		
		/*LocalFileSystem lfs = new LocalFileSystem();
		
		Configuration conf = lfs.getConf();
		JobConf j = new JobConf(conf);
		SegmentReader reader =new SegmentReader();
		reader.configure(j);
		Path in = new Path("D:/crawl/segments/20151204192823/parse_text/part-00000");
		Path out = new Path("D:/segdump");
		reader.dump(in, out);
		System.out.println("ok");*/
		
		/*Configuration conf = new Configuration();
//		
//		JobConf sortJob = new NutchJob(conf);
//		
//		FileInputFormat.addInputPath(sortJob, new Path(""));
		
		
		
		Injector inject = new Injector(conf);
		
		inject.inject(new Path("/crawl/"),new Path("/urls/urls/"));*/
		
//		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
//		TokenStream ts = analyzer.tokenStream("body", new FileReader(new File("D:\\hadoop\\20news-bydate-train\\comp.graphics\\37261")));
//		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
//		ts.reset();
//		while(ts.incrementToken()){
////			char[] termbuffer = termAtt.buffer();
////			String w = new String(termbuffer,1,termAtt.length());
//			String w = ts.getAttribute(CharTermAttribute.class).toString();
//			System.out.println(w);
//		}
//		ts.end();
//		ts.close();
		
		
//		TrainNewsGroups.main(new String[]{"D:\\hadoop\\20news-bydate-train"});
		
//		String[] arg = {"--input","D:\\hadoop\\20news-bydate-test","--model","D:\\tmp\\news-group.model"};
//		TestNewsGroups.main(arg);
//		
		
//		List<String> symbols = new ArrayList<String>();
//		symbols.add("x1");
//		symbols.add("x2");
//		symbols.add("x3");
//		symbols.add("x4");
//		
//		ConfusionMatrix mx = new ConfusionMatrix(symbols, "unkonw");
//		mx.addInstance("x1", "x1");
//		mx.addInstance("x2", "x2");
//		mx.addInstance("x3", "x3");
//		mx.addInstance("x4", "x4");
//		
//		System.out.printf("%s\n\n",mx.toString());
//		
		
		/*Multiset<String> set = ConcurrentHashMultiset.create();
		set.add("hello");
		set.add("you");
		set.add("hello");
		
		System.out.println(set.count("hello"));
		
		for (String string : set.elementSet()) {
			System.out.println(string);
		}*/
		
		Vector v1 = new DenseVector(new double[]{1,5,6,9,8,3,12});
		System.out.println(v1);
		Vector v2 = new DenseVector(new double[]{2,5,4,2,3,10,12});
		System.out.println(v2);
		Vector sum = v1.assign(v2, Functions.PLUS);
		System.out.println(sum);
		System.out.println(v1);
		System.out.println(v1.zSum());
		
		
	}

}
