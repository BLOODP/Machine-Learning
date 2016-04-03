package cn.edu.cqupt.kmeans.mahout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.lucene.benchmark.utils.ExtractReuters;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.utils.SplitInput;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import cn.edu.cqupt.rubic_core.io.HDFSConnection;

public class TextVectorDemo {

	public static void main(String[] args) throws IOException {
		
		
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("core-site.xml"));
		conf.addResource(new Path("mapred-site.xml"));
		
		FileSystem fs = FileSystem.get(conf);
		
		/*Path in = new Path("hdfs://master:9000/user/hadoop/reuters/");
		
	
		FileStatus[] status = fs.listStatus(in);
		String inpath;
		String outpath;
		 for(int i=0; i<status.length; i++){
			 inpath = status[i].getPath().toString();
	            System.out.println(inpath);
	            
	            int index = inpath.lastIndexOf('/');
	            outpath = inpath.substring(index+1);
	            System.out.println(outpath);
	            
	            fs.copyToLocalFile(new Path(inpath), new Path("D:/reuters/"+outpath));
	        }
		 fs.close();
		 System.out.println("ok");
		 
		 File reuter = new File("D:/reuters");
		 File[] reuters = reuter.listFiles();
		 String reuterName;
		 for (File file : reuters) {
			 reuterName = file.getAbsolutePath();
			 if(reuterName.endsWith("crc")){
				 new File(reuterName).deleteOnExit();
			 }
		}
		 
		File input = new File("D:/reuters");
		File output = new File("D:/reuters-extracted");
		if(!output.exists()){
			output.mkdir();
		}
		
		ExtractReuters extractor = new ExtractReuters(input, output);
		extractor.extract();
		System.out.println("ok");
		*/
		
	/*	
		File root = new File("D:/reuters-extracted");
		File[] files = root.listFiles();
		File src;
		Path dst;
		String inp;
		String outp;
		FSDataOutputStream fsos;
		for (File file : files) {
			System.out.println(file.getAbsolutePath());
			inp = file.getAbsolutePath();
			src = new File(inp);
			FileInputStream fins = new FileInputStream(src);
			int index = inp.lastIndexOf('\\');
			outp = inp.substring(index+1);
			dst = new Path("reuters-extracted/"+outp);
			fsos = fs.create(dst, true);
			IOUtils.copyBytes(fins, fsos, 2048, true);
		}*/
		
//		Path dst = new Path("/uesr/hadoop/extracted");
//		fs.mkdirs(dst);
//		System.out.println("ok");
//		Path src = new Path("D:/reuters-extracted");
//		fs.copyFromLocalFile(src, dst);
//		System.out.println("ok");
		
		
		
		String[] args1 = {"-c", "UTF-8", "-i", "hdfs://master:9000/uesr/hadoop/extracted/reuters-extracted", "-o",
		 "hdfs://master:9000/uesr/hadoop/extracted/reuters-seqfiles"};
		
		
		try {
			SequenceFilesFromDirectory.main(args1);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.println("SequenceFilesFromDirectory ok");
		
		String[] args2 = {"-i","hdfs://master:9000/uesr/hadoop/extracted/reuters-seqfiles","-o","hdfs://master:9000/uesr/hadoop/extracted/reuters-vectors","-ow"};
		
		try {
			SparseVectorsFromSequenceFiles.main(args2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("SparseVectorsFromSequenceFiles ok");
		
		
		String[] arg = {"-i","hdfs://master:9000/uesr/hadoop/extracted/reuters-vectors/tfidf-vectors","-c","hdfs://master:9000/uesr/hadoop/extracted/reuters-initial-clusters","-o","hdfs://master:9000/uesr/hadoop/extracted/reuters-kmeans-clusters","-dm","org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure","-cd","1.0","-k","20","-x","20","-cl"};
		try {
			KMeansDriver.main(arg);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("KMeansDriver ok");
	}
	
}
