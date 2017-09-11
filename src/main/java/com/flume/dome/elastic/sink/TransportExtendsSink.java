package com.flume.dome.elastic.sink;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLIENT_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLIENT_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CLIENT_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_NAME_BUILDER_CLASS;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_SERIALIZER_CLASS;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_TTL;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.HOSTNAMES;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME_BUILDER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME_BUILDER_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL_REGEX;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.ElasticSearchSink;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClient;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

//注意依懒的版本，是0.90.1
//import org.elasticsearch.common.io.BytesStream;

/**
 * 对flume-ng-elasticsearch-sink 进行扩展
 * <p>
 * k=v转josn或数据加工处理 *
 * <p>
 * 不可用，需要升级版本的依懒
 * 
 * @author rui
 * @date 2017/7/21
 *
 */

@Deprecated
public class TransportExtendsSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(TransportExtendsSink.class);

	// Used for testing
	private boolean isLocal = false;
	private final CounterGroup counterGroup = new CounterGroup();

	private static final int defaultBatchSize = 100;

	private int batchSize = defaultBatchSize;
	private long ttlMs = DEFAULT_TTL;
	private String clusterName = DEFAULT_CLUSTER_NAME;
	private String indexName = DEFAULT_INDEX_NAME;
	private String indexType = DEFAULT_INDEX_TYPE;
	private String clientType = DEFAULT_CLIENT_TYPE;
	private final Pattern pattern = Pattern.compile(TTL_REGEX, Pattern.CASE_INSENSITIVE);
	private Matcher matcher = pattern.matcher("");

	private String[] serverAddresses = null;

	private ElasticSearchClient client = null;
	private Context elasticSearchClientContext = null;

	private ElasticSearchIndexRequestBuilderFactory indexRequestFactory;
	private ElasticSearchEventSerializer eventSerializer;
	private IndexNameBuilder indexNameBuilder;
	private SinkCounter sinkCounter;
	private String josnTo = "true";// 是否转换json

	/**
	 * Create an {@link ElasticSearchSink} configured using the supplied
	 * configuration
	 */
	public TransportExtendsSink() {
		this(false);
	}

	/**
	 * Create an {@link ElasticSearchSink}
	 * </p>
	 * 
	 * @param isLocal
	 *            If <tt>true</tt> sink will be configured to only talk to an
	 *            ElasticSearch instance hosted in the same JVM, should always
	 *            be false is production
	 * 
	 */
	@VisibleForTesting
	TransportExtendsSink(boolean isLocal) {
		this.isLocal = isLocal;
	}

	@VisibleForTesting
	String[] getServerAddresses() {
		return serverAddresses;
	}

	@VisibleForTesting
	String getClusterName() {
		return clusterName;
	}

	@VisibleForTesting
	String getIndexName() {
		return indexName;
	}

	@VisibleForTesting
	String getIndexType() {
		return indexType;
	}

	@VisibleForTesting
	long getTTLMs() {
		return ttlMs;
	}

	@VisibleForTesting
	ElasticSearchEventSerializer getEventSerializer() {
		return eventSerializer;
	}

	@VisibleForTesting
	IndexNameBuilder getIndexNameBuilder() {
		return indexNameBuilder;
	}

	public Status process() throws EventDeliveryException {
		logger.debug("processing...");
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction txn = channel.getTransaction();
		// 数据集合
		List<JSONObject> actions = Lists.newArrayList();
		String content;
		try {
			txn.begin();
			int count;
			for (count = 0; count < batchSize; ++count) {
				Event event = channel.take();// 从通道中获取数据
				if (event == null) {
					break;
				}
				content = new String(event.getBody(), "utf-8");
				logger.info("estaic josnTo {},src content:{}", josnTo, content);
				if (josnTo != null && "true".equals(josnTo)) {
					// 把文本按行分隔，把kv转json
					actions = com.flume.dome.xutils.ConverData.conver(content);
				} else {
					// 加工数据
					actions = com.flume.dome.xutils.ConverData.converStr(content);
				}
				if (actions == null || actions.size() <= 0) {
					break;
				}

				Integer sid = -2;
				// 获取hader 信息
				Map<String, String> headMap = event.getHeaders();
				if (headMap.containsKey("serverId")) {
					String sid_str = headMap.get("serverId");
					if (StringUtils.isNotBlank(sid_str)) {
						sid = Integer.parseInt(sid_str);
					}
				}
				// 转字符串
				StringBuffer sb = new StringBuffer();
				for (JSONObject jobj : actions) {
					jobj.put("server_id", sid);
					if (StringUtils.isBlank(jobj.toJSONString())) {
						continue;
					}
					sb.append(jobj.toJSONString());
				}

				if (StringUtils.isBlank(sb.toString())) {
					break;
				}
				// 加入event
				event.setBody(sb.toString().getBytes());
				logger.info("estaic sink 处理后的数据 content:{}", sb.toString());
				// 多路复制时可以根据不同头部k,来分发不同的流向
				// Map<String,String> headers=new HashMap<String,String>();
				// headers.put("state", "multiplexing-es");
				// headers.put("state", "multiplexing-db");
				// event.setHeaders(headers);
				String realIndexType = BucketPath.escapeString(indexType, event.getHeaders());

				logger.info("estaic sink 处理后的数据 content:{}", sb.toString());

				client.addEvent(event, indexNameBuilder, realIndexType, ttlMs);
				logger.info("indexNameBuilder.getIndexName(event) 索引:{},type:{}", indexNameBuilder.getIndexName(event),
						realIndexType);

			}

			if (count <= 0) {
				sinkCounter.incrementBatchEmptyCount();
				counterGroup.incrementAndGet("channel.underflow");
				status = Status.BACKOFF;
			} else {
				if (count < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
					status = Status.BACKOFF;
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(count);
				client.execute();
			}
			txn.commit();
			sinkCounter.addToEventDrainSuccessCount(count);
			counterGroup.incrementAndGet("transaction.success");
		} catch (Throwable ex) {
			try {
				txn.rollback();
				counterGroup.incrementAndGet("transaction.rollback");
			} catch (Exception ex2) {
				logger.error("Exception in rollback. Rollback might not have been successful.", ex2);
			}

			if (ex instanceof Error || ex instanceof RuntimeException) {
				logger.error("Failed to commit transaction. Transaction rolled back.", ex);
				Throwables.propagate(ex);
			} else {
				logger.error("Failed to commit transaction. Transaction rolled back.", ex);
				throw new EventDeliveryException("Failed to commit transaction. Transaction rolled back.", ex);
			}
		} finally {
			txn.close();
		}
		return status;
	}

	// @Override
	public void configure(Context context) {
		if (!isLocal) {
			if (StringUtils.isNotBlank(context.getString(HOSTNAMES))) {
				serverAddresses = StringUtils.deleteWhitespace(context.getString(HOSTNAMES)).split(",");
			}
			Preconditions.checkState(serverAddresses != null && serverAddresses.length > 0,
					"Missing Param:" + HOSTNAMES);
		}

		if (StringUtils.isNotBlank(context.getString(INDEX_NAME))) {
			this.indexName = context.getString(INDEX_NAME);
		}

		if (StringUtils.isNotBlank(context.getString(INDEX_TYPE))) {
			this.indexType = context.getString(INDEX_TYPE);
		}

		if (StringUtils.isNotBlank(context.getString(CLUSTER_NAME))) {
			this.clusterName = context.getString(CLUSTER_NAME);
		}

		if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
			this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
		}

		if (StringUtils.isNotBlank(context.getString(TTL))) {
			this.ttlMs = parseTTL(context.getString(TTL));
			Preconditions.checkState(ttlMs > 0, TTL + " must be greater than 0 or not set.");
		}

		if (StringUtils.isNotBlank(context.getString(CLIENT_TYPE))) {
			clientType = context.getString(CLIENT_TYPE);
		}

		josnTo = context.getString("josnTo");

		elasticSearchClientContext = new Context();
		elasticSearchClientContext.putAll(context.getSubProperties(CLIENT_PREFIX));

		String serializerClazz = DEFAULT_SERIALIZER_CLASS;
		if (StringUtils.isNotBlank(context.getString(SERIALIZER))) {
			serializerClazz = context.getString(SERIALIZER);
		}

		Context serializerContext = new Context();
		serializerContext.putAll(context.getSubProperties(SERIALIZER_PREFIX));

		try {
			@SuppressWarnings("unchecked")
			Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class.forName(serializerClazz);
			Configurable serializer = clazz.newInstance();

			if (serializer instanceof ElasticSearchIndexRequestBuilderFactory) {
				indexRequestFactory = (ElasticSearchIndexRequestBuilderFactory) serializer;
				indexRequestFactory.configure(serializerContext);
			} else if (serializer instanceof ElasticSearchEventSerializer) {
				eventSerializer = (ElasticSearchEventSerializer) serializer;
				eventSerializer.configure(serializerContext);
			} else {
				throw new IllegalArgumentException(serializerClazz + " is not an ElasticSearchEventSerializer");
			}
		} catch (Exception e) {
			logger.error("Could not instantiate event serializer.", e);
			Throwables.propagate(e);
		}

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		String indexNameBuilderClass = DEFAULT_INDEX_NAME_BUILDER_CLASS;
		if (StringUtils.isNotBlank(context.getString(INDEX_NAME_BUILDER))) {
			indexNameBuilderClass = context.getString(INDEX_NAME_BUILDER);
		}

		Context indexnameBuilderContext = new Context();
		serializerContext.putAll(context.getSubProperties(INDEX_NAME_BUILDER_PREFIX));

		try {
			@SuppressWarnings("unchecked")
			Class<? extends IndexNameBuilder> clazz = (Class<? extends IndexNameBuilder>) Class
					.forName(indexNameBuilderClass);
			indexNameBuilder = clazz.newInstance();
			indexnameBuilderContext.put(INDEX_NAME, indexName);
			indexNameBuilder.configure(indexnameBuilderContext);
		} catch (Exception e) {
			logger.error("Could not instantiate index name builder.", e);
			Throwables.propagate(e);
		}

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		Preconditions.checkState(StringUtils.isNotBlank(indexName), "Missing Param:" + INDEX_NAME);
		Preconditions.checkState(StringUtils.isNotBlank(indexType), "Missing Param:" + INDEX_TYPE);
		Preconditions.checkState(StringUtils.isNotBlank(clusterName), "Missing Param:" + CLUSTER_NAME);
		Preconditions.checkState(batchSize >= 1, BATCH_SIZE + " must be greater than 0");
	}

	@Override
	public void start() {
		ElasticSearchClientFactory clientFactory = new ElasticSearchClientFactory();

		logger.info("ElasticSearch sink {} started");
		sinkCounter.start();
		try {
			if (isLocal) {
				client = clientFactory.getLocalClient(clientType, eventSerializer, indexRequestFactory);
			} else {
				client = clientFactory.getClient(clientType, serverAddresses, clusterName, eventSerializer,
						indexRequestFactory);
				client.configure(elasticSearchClientContext);
			}
			sinkCounter.incrementConnectionCreatedCount();
		} catch (Exception ex) {
			ex.printStackTrace();
			sinkCounter.incrementConnectionFailedCount();
			if (client != null) {
				client.close();
				sinkCounter.incrementConnectionClosedCount();
			}
		}

		super.start();
	}

	@Override
	public void stop() {
		logger.info("ElasticSearch sink {} stopping");
		if (client != null) {
			client.close();
		}
		sinkCounter.incrementConnectionClosedCount();
		sinkCounter.stop();
		super.stop();
	}

	/*
	 * Returns TTL value of ElasticSearch index in milliseconds when TTL
	 * specifier is "ms" / "s" / "m" / "h" / "d" / "w". In case of unknown
	 * specifier TTL is not set. When specifier is not provided it defaults to
	 * days in milliseconds where the number of days is parsed integer from TTL
	 * string provided by user. <p> Elasticsearch supports ttl values being
	 * provided in the format: 1d / 1w / 1ms / 1s / 1h / 1m specify a time unit
	 * like d (days), m (minutes), h (hours), ms (milliseconds) or w (weeks),
	 * milliseconds is used as default unit.
	 * http://www.elasticsearch.org/guide/reference/mapping/ttl-field/.
	 * 
	 * @param ttl TTL value provided by user in flume configuration file for the
	 * sink
	 * 
	 * @return the ttl value in milliseconds
	 */
	private long parseTTL(String ttl) {
		matcher = matcher.reset(ttl);
		while (matcher.find()) {
			if (matcher.group(2).equals("ms")) {
				return Long.parseLong(matcher.group(1));
			} else if (matcher.group(2).equals("s")) {
				return TimeUnit.SECONDS.toMillis(Integer.parseInt(matcher.group(1)));
			} else if (matcher.group(2).equals("m")) {
				return TimeUnit.MINUTES.toMillis(Integer.parseInt(matcher.group(1)));
			} else if (matcher.group(2).equals("h")) {
				return TimeUnit.HOURS.toMillis(Integer.parseInt(matcher.group(1)));
			} else if (matcher.group(2).equals("d")) {
				return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
			} else if (matcher.group(2).equals("w")) {
				return TimeUnit.DAYS.toMillis(7 * Integer.parseInt(matcher.group(1)));
			} else if (matcher.group(2).equals("")) {
				logger.info("TTL qualifier is empty. Defaulting to day qualifier.");
				return TimeUnit.DAYS.toMillis(Integer.parseInt(matcher.group(1)));
			} else {
				logger.debug("Unknown TTL qualifier provided. Setting TTL to 0.");
				return 0;
			}
		}
		logger.info("TTL not provided. Skipping the TTL config by returning 0.");
		return 0;
	}
}
