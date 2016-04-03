package cn.edu.cqupt.kmeans.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ReadFileDirectory {
	
	public static void main(String[] args) throws IOException {
		
		File base = new File("D:\\hadoop\\20news-bydate-train");
		File output = new File("D:\\20news-bydate.txt");
		FileOutputStream outputStream = new FileOutputStream(output);
		
		File[] directories = base.listFiles();
		
		for (File directory : directories) {
			File[] files = directory.listFiles();
			
			for (File file : files) {
				
				String path = file.getAbsolutePath()+"\n";
				System.out.print(path);
				outputStream.write(path.getBytes());
			}
			
		}
		outputStream.close();
		
	}

}
