package cn.edu.cqupt.kmeans.test;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 8888);
		try {
			ClientProtocol proxy = (ClientProtocol) RPC.getProxy(ClientProtocol.class, ClientProtocol.versionId, addr, conf);
			int result = proxy.add(5, 6);
			System.out.println(result);
			String s = proxy.echo("motherfucker");
			System.out.println(s);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
