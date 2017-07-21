package com.flume.dome.conver;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * @date 2017/07/19
 * @author rui
 *
 */
public class EsUtil {
	

	public static final String httpHost = "192.168.20.243";
	public static final Integer port = 9200;
	public static final String username = "elastic";
	public static final String password = "123456";

	final static CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

	/**
	 * 获取客户端
	 * 
	 * @return
	 */
	public static RestClient getRestClint() {
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		RestClient restClient = RestClient.builder(new HttpHost(httpHost, port))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				}).build();
		return restClient;
	}

}
