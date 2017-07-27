
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.flume.dome.elastic.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClient;
import org.apache.flume.sink.elasticsearch.client.RoundRobinList;
import org.apache.http.Consts;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class EsHttpClientExt implements ElasticSearchClient {

	private static final String INDEX_OPERATION_NAME = "index";
	private static final String INDEX_PARAM = "_index";
	private static final String TYPE_PARAM = "_type";
	private static final String TTL_PARAM = "_ttl";
	private static final String BULK_ENDPOINT = "_bulk";

	private static final Logger logger = LoggerFactory.getLogger(EsHttpClientExt.class);

	private final ElasticSearchEventSerializer serializer;
	private final RoundRobinList<String> serversList;

	private StringBuilder bulkBuilder;
	private HttpClient httpClient;

	private Integer port = 9200;
	private String username;
	private String password;

	public EsHttpClientExt(String[] hostNames, ElasticSearchEventSerializer serializer, String username,
			String password, Integer port) {

		for (int i = 0; i < hostNames.length; ++i) {
			if (!hostNames[i].contains("http://") && !hostNames[i].contains("https://")) {
				hostNames[i] = "http://" + hostNames[i];
			}
		}
		this.serializer = serializer;

		serversList = new RoundRobinList<String>(Arrays.asList(hostNames));
		// httpClient = new DefaultHttpClient();
		// CloseableHttpClient httpClient = HttpClients.createDefault();
		httpClient = HttpClients.createDefault();
		bulkBuilder = new StringBuilder();
		this.username = username;
		this.password = password;
		this.port = port;
	}

	// @VisibleForTesting
	// public ElasticSearchRestClient(String[] hostNames,
	// ElasticSearchEventSerializer serializer, HttpClient client) {
	// this(hostNames, serializer);
	// httpClient = client;
	// }

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
		HttpResponse response = null;
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
			httpRequest.setEntity(new StringEntity(entity,Consts.UTF_8));
			logger.debug("打印出 url: " + url);
			logger.debug("打印出 entity: " + entity);
			// 或都 header写死
			httpRequest.setHeader("Authorization", "Basic ZWxhc3RpYzoxMjM0NTY=");
			httpRequest.setHeader("Cookie",
					"sid=Fe26.2**1cb1377880e0bfe8ca5c210dc0293b4ccac3ac746d9a7d5ae99554282b34abaa*Yiuw0D4KTDyUrkiJKYYScA*ALh7r9ombireJ0er01_46_MAoK8Vjsb_hCZB9mBy_P7Z3JZtf_K-TUdlwKYnGrVIUPC9I0fEFTMmwV7AzXExmw**554d722569d9dea45f5275b077d7d39cd04c59f50fe1e25b5abdb36a7809898b*M7hE0_CgUu7-AhYWQayd0eeQPCdIyih2EWuOCeSavro");
			httpRequest.setHeader("Content-Type", "application/json; charset=UTF-8");
			// Authorization =Basic ZWxhc3RpYzoxMjM0NTY=
			// Cookie
			// =sid=Fe26.2**1cb1377880e0bfe8ca5c210dc0293b4ccac3ac746d9a7d5ae99554282b34abaa*Yiuw0D4KTDyUrkiJKYYScA*ALh7r9ombireJ0er01_46_MAoK8Vjsb_hCZB9mBy_P7Z3JZtf_K-TUdlwKYnGrVIUPC9I0fEFTMmwV7AzXExmw**554d722569d9dea45f5275b077d7d39cd04c59f50fe1e25b5abdb36a7809898b*M7hE0_CgUu7-AhYWQayd0eeQPCdIyih2EWuOCeSavro
			// 设置用户密码
			// HttpHost proxy = new HttpHost("proxy", port);
			// BasicScheme proxyAuth = new BasicScheme();
			// // Make client believe the challenge came form a proxy
			// proxyAuth.processChallenge(new BasicHeader(AUTH.PROXY_AUTH,
			// "BASIC realm=default"));
			// BasicAuthCache authCache = new BasicAuthCache();
			// authCache.put(proxy, proxyAuth);
			// CredentialsProvider credsProvider = new
			// BasicCredentialsProvider();
			// credsProvider.setCredentials(new AuthScope(proxy), new
			// UsernamePasswordCredentials(username, password));
			// HttpClientContext context = HttpClientContext.create();
			// context.setAuthCache(authCache);
			// context.setCredentialsProvider(credsProvider);

			response = httpClient.execute(httpRequest);
			statusCode = response.getStatusLine().getStatusCode();
			logger.info("Status code from elasticsearch: " + statusCode);

			logger.debug("返回状态  Status code from elasticsearch: " + statusCode);
			if (response.getEntity() != null) {
				logger.debug("返回信息 Status message from elasticsearch response: "
						+ EntityUtils.toString(response.getEntity(), "UTF-8"));
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
