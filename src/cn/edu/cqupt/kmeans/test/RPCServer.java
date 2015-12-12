package cn.edu.cqupt.kmeans.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer {
	
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		try {
			Server server = RPC.getServer(new ClientProtocolImpl(), "127.0.0.1", 8888, 1, false, conf);
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
