package com.flume.dome.elastic.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClient;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchRestClient;
import org.apache.flume.sink.elasticsearch.client.RoundRobinList;
import org.apache.http.HttpStatus;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flume.dome.conver.EsUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

/**
 * {@link org.apache.flume.sink.elasticsearch.client.ElasticSearchRestClient}
 * 
 * @desc 对flume-ng-elasticsearch-sink 进行扩展
 *       <p>
 *       k=v转josn或数据加工处理 *
 * 
 * @author rui
 * @date 2017/7/21
 */

public class EsRestSink implements ElasticSearchClient {

	private static final String INDEX_OPERATION_NAME = "index";
	private static final String INDEX_PARAM = "_index";
	private static final String TYPE_PARAM = "_type";
	private static final String TTL_PARAM = "_ttl";
	private static final String BULK_ENDPOINT = "_bulk";

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

	private final ElasticSearchEventSerializer serializer;
	private final RoundRobinList<String> serversList;

	private StringBuilder bulkBuilder;
//	private HttpClient httpClient;
//	private CloseableHttpClient httpClient;
	
	
	final static CredentialsProvider credentialsProvider = new BasicCredentialsProvider();


	public EsRestSink(String[] hostNames, ElasticSearchEventSerializer serializer) {

		for (int i = 0; i < hostNames.length; ++i) {
			if (!hostNames[i].contains("http://") && !hostNames[i].contains("https://")) {
				hostNames[i] = "http://" + hostNames[i];
			}
		}
		this.serializer = serializer;

		serversList = new RoundRobinList<String>(Arrays.asList(hostNames));
//		httpClient = new DefaultHttpClient();
		bulkBuilder = new StringBuilder();
	}
	
	
	/**
	 * 获取客户端
	 * 
	 * @return
	 */
//	public static RestClient getRestClint() {
//		RestClient restClient = RestClient.builder(new HttpHost(httpHost, port))
//				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//					@Override
//					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//					}
//				}).build();
//		return restClient;
//	}
	

	@VisibleForTesting
	public EsRestSink(String[] hostNames, ElasticSearchEventSerializer serializer, HttpClient client) {
		this(hostNames, serializer);
//		httpClient = client;
	}

	public void configure(Context context) {
	}

	public void close() {
	}

	public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType, long ttlMs)
			throws Exception {
		BytesReference content = serializer.getContentBuilder(event).bytes();
		Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
		Map<String, String> indexParameters = new HashMap<String, String>();
		indexParameters.put(INDEX_PARAM, indexNameBuilder.getIndexName(event));
		indexParameters.put(TYPE_PARAM, indexType);
		if (ttlMs > 0) {
			indexParameters.put(TTL_PARAM, Long.toString(ttlMs));
		}
		parameters.put(INDEX_OPERATION_NAME, indexParameters);

		Gson gson = new Gson();
		synchronized (bulkBuilder) {
			bulkBuilder.append(gson.toJson(parameters));
			bulkBuilder.append("\n");
			bulkBuilder.append(content.toBytesArray().toUtf8());
			bulkBuilder.append("\n");
		}
	}

	public void execute() throws Exception {
		int statusCode = 0, triesCount = 0;
//		HttpResponse response = null;
		org.elasticsearch.client.Response  response=null;
		String entity;
		synchronized (bulkBuilder) {
			entity = bulkBuilder.toString();
			bulkBuilder = new StringBuilder();
		}
		while (statusCode != HttpStatus.SC_OK && triesCount < serversList.size()) {
			triesCount++;
			String host = serversList.get();
			String url = host + "/" + BULK_ENDPOINT;
			HttpPost httpRequest = new HttpPost(url);
			httpRequest.setEntity(new StringEntity(entity));
			logger.info("打印出 host: " + host);
			response = EsUtil.getRestClint().performRequest("POST", "/game_log/zl_log_info/_bulk",
					Collections.singletonMap("pretty", "true"), new StringEntity(entity));
//			response = httpClient.execute(httpRequest);
			statusCode = response.getStatusLine().getStatusCode();
			logger.info("Status code from elasticsearch: " + statusCode);
			if (response.getEntity() != null) {
				logger.debug(
						"Status message from elasticsearch: " + EntityUtils.toString(response.getEntity(), "UTF-8"));
			}
		}

		if (statusCode != HttpStatus.SC_OK) {
			if (response.getEntity() != null) {
				throw new EventDeliveryException(EntityUtils.toString(response.getEntity(), "UTF-8"));
			} else {
				throw new EventDeliveryException("Elasticsearch status code was: " + statusCode);
			}
		}
	}

}
