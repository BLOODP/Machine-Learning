package cn.edu.cqupt.kmeans.test;

import java.io.IOException;

public class ClientProtocolImpl implements ClientProtocol {

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return ClientProtocol.versionId;
	}

	@Override
	public String echo(String value) {
		return "you "+value;
	}

	@Override
	public int add(int v1, int v2) {
		return v1+v2;
	}

}
