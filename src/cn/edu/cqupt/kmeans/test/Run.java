package cn.edu.cqupt.kmeans.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Run implements Runnable {
	
	public static Configuration conf = new Configuration();
	public static FileSystem fs;
	private List<String> list;
	private File file;
	private FileInputStream in;
	private Path outpout;
	
	public Run(List<String> list){
		this.list = list;
	}
	
	@Override
	public void run() {
		
		for (String string : list) {
			System.out.println(string);
			file = new File(string);
			string = string.replace('\\', '/');
			String path = "/Rubic/62/20news-bydate-train/";
			String[] strs = string.split("/");
			int size = strs.length;
			path = path+strs[size-2]+"/"+strs[size-1];
			try {
				in = new FileInputStream(file);
				outpout = new Path(path);
				fs = FileSystem.get(conf);
				FSDataOutputStream fos = fs.create(outpout, true);
				IOUtils.copyBytes(in, fos, conf);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		

	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		/*File file = new File("D:\\20news-bydate.txt");
		List<String> arr = new ArrayList<>();
		FileInputStream in = new FileInputStream(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		String line = br.readLine();
		while(line!=null){
			arr.add(line);
			line = br.readLine();
		}
		in.close();
		br.close();
		
		int i1 = arr.size()/3;
		int i2 = (arr.size()*2)/3;
		List<String> l1 = arr.subList(0, i1);
		List<String> l2 = arr.subList(i1, i2);
		List<String> l3 = arr.subList(i2, arr.size());
		
//		long begin = System.currentTimeMillis();
//		for (String string : arr) {
//			System.out.println(string);
//		}
//		long end = System.currentTimeMillis();
//		System.out.println((end-begin));
		long begin = System.currentTimeMillis();
		Run r1 = new Run(l1);
		Run r2 = new Run(l2);
		Run r3 = new Run(l3);
		
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		Thread t3 = new Thread(r3);
		
		t1.start();
		t2.start();
		t3.start();
		Thread.sleep(300);
		long end = System.currentTimeMillis();
		System.out.println((end-begin));*/
		long begin = System.currentTimeMillis();
		test();
//		Thread.sleep(600);
		
		long end = System.currentTimeMillis();
		System.out.println((end-begin));
		System.out.println("end");
		
	}

	public static void test() throws IOException{
		File file = new File("D:\\20news-bydate.txt");
		List<String> arr = new ArrayList<>();
		FileInputStream in = new FileInputStream(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		String line = br.readLine();
		while(line!=null){
			arr.add(line);
			line = br.readLine();
		}
		in.close();
		br.close();
		
		int i1 = arr.size()/3;
		int i2 = (arr.size()*2)/3;
		List<String> l1 = arr.subList(0, i1);
		List<String> l2 = arr.subList(i1, i2);
		List<String> l3 = arr.subList(i2, arr.size());
		
//		long begin = System.currentTimeMillis();
//		for (String string : arr) {
//			System.out.println(string);
//		}
//		long end = System.currentTimeMillis();
//		System.out.println((end-begin));
//		long begin = System.currentTimeMillis();
		Run r1 = new Run(l1);
		Run r2 = new Run(l2);
		Run r3 = new Run(l3);
		
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		Thread t3 = new Thread(r3);
		
		t1.start();
		t2.start();
		t3.start();
		
//		Thread.sleep(300);
//		long end = System.currentTimeMillis();
//		System.out.println((end-begin));
	}
	
}
