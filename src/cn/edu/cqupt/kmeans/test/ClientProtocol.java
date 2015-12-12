package cn.edu.cqupt.kmeans.test;


import org.apache.hadoop.ipc.VersionedProtocol;

public interface ClientProtocol extends VersionedProtocol {

	public static final long versionId = 1L;
	String echo(String value);
	int add(int v1,int v2);
	
	
}
