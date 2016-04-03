package cn.edu.cqupt.kmeans.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.mahout.common.HadoopUtil;

public class UploadDataSetToHDFS {
	
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		File hly_temp_normal = new File("D:\\donut.csv");
		FileInputStream fin = new FileInputStream(hly_temp_normal);
		
		Path outpath = new Path("hdfs://master1:9000/Rubic/62/dataset/donut.csv");
		HadoopUtil.delete(conf, outpath);
		FSDataOutputStream fos = fs.create(outpath, true);
		IOUtils.copyBytes(fin, fos, conf);
		System.out.println("ok.....");
		
	}

}
