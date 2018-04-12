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

package com.flume.dome.mysink;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 重写avro协议，转换特定json到银河平台
 * 
 * @author ruiliang
 * @date 2018/01/31
 * 
 *       extends AbstractRpcSink
 */
public class AvroSinkExt extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(AvroSinkExt.class);
	private String hostname;
	private Integer port;
	private RpcClient client;
	private Properties clientProps;
	private SinkCounter sinkCounter;
	private int cxnResetInterval;
	private AtomicBoolean resetConnectionFlag;
	private final int DEFAULT_CXN_RESET_INTERVAL = 0;
	private final ScheduledExecutorService cxnResetExecutor = Executors.newSingleThreadScheduledExecutor(
			new ThreadFactoryBuilder().setNameFormat("Rpc Sink Reset Thread").build());

	protected RpcClient initializeRpcClient(Properties props) {
		logger.info("Attempting to create Avro Rpc client.");
		return RpcClientFactory.getInstance(props);
	}

	@Override
	public void configure(Context context) {
		clientProps = new Properties();
		hostname = context.getString("hostname");
		port = context.getInteger("port");

		Preconditions.checkState(hostname != null, "No hostname specified");
		Preconditions.checkState(port != null, "No port specified");

		clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
		clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1", hostname + ":" + port);

		for (Entry<String, String> entry : context.getParameters().entrySet()) {
			clientProps.setProperty(entry.getKey(), entry.getValue());
		}

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
		cxnResetInterval = context.getInteger("reset-connection-interval", DEFAULT_CXN_RESET_INTERVAL);
		if (cxnResetInterval == DEFAULT_CXN_RESET_INTERVAL) {
			logger.info("Connection reset is set to " + String.valueOf(DEFAULT_CXN_RESET_INTERVAL)
					+ ". Will not reset connection to next hop");
		}
	}

	public static void main(String[] args) {

		String s="25740.0";
		System.out.println(s); 
		System.out.println(StringUtils.isNumeric(s));
		System.out.println(StringUtils.isNumericSpace(s));

//		System.out.println(StringUtils.isNumeric(null));
//		System.out.println(StringUtils.isNumeric(""));
//
//		System.out.println(StringUtils.isNumeric("10"));
//
//		System.out.println(Long.parseLong(""));
	}

	/**
	 * 转换平台格式
	 */
	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		if (resetConnectionFlag.get()) {
			resetConnection();
			// if the time to reset is long and the timeout is short
			// this may cancel the next reset request
			// this should however not be an issue
			resetConnectionFlag.set(false);
		}

		try {
			transaction.begin();
			verifyConnection();
			List<Event> batch = Lists.newLinkedList();
			for (int i = 0; i < client.getBatchSize(); i++) {
				Event event = channel.take();
				logger.debug("event...{}", event);
				if (event == null) {
					break;
				}
				// 取出event_body
				String event_body = new String(event.getBody(), "utf-8");
				logger.debug("content...{}", event_body);
				JSONObject json_src = (JSONObject) JSONObject.parse(event_body);

				// 根节点
				JSONObject root = new JSONObject();
				root.put("who", "jyqy");
				root.put("platform", "app");
				// 时间ts
				// if (json_src.containsKey("ts")) {
				// Object ts = json_src.get("ts");
				// // 如果为null "" 或中文 默认=0
				// if (StringUtils.isNumeric(ts.toString()) &&
				// !"".equals(ts.toString())) {
				// root.put("when",
				// Long.parseLong(json_src.get("ts").toString()));
				// } else {
				// root.put("when", System.currentTimeMillis());
				// }
				// json_src.remove("ts");
				// } else {
				// root.put("when", System.currentTimeMillis());
				// }
				root.put("when", System.currentTimeMillis());

				// file
				if (json_src.containsKey("file")) {
					root.put("what", json_src.get("file"));
					json_src.remove("file");
				}

				// 共用节点
				JSONObject player = new JSONObject();
				// 操作系统
				if (json_src.containsKey("os") && StringUtils.isNotBlank(json_src.getString("os").toString())) {
					String os = json_src.get("os").toString();
					if (os.toLowerCase().equals("android")) {
						player.put("os", 1);
					} else if (os.toLowerCase().equals("ios")) {
						player.put("os", 2);
					} else {
						player.put("os", 0);
					}
					json_src.remove("os");
				}
				// ip
				if (json_src.containsKey("ip")) {
					player.put("cip", json_src.get("ip"));
					json_src.remove("ip");
				}
				// 服务器
				if (json_src.containsKey("server_id")) {
					player.put("server", json_src.get("server_id"));
					json_src.remove("server_id");
				}
				// 怅号
				Object account = 0;
				if (json_src.containsKey("account")) {
					account = json_src.get("account");
					if (null == account || "".equals(account.toString())) {// 空默认0
						account = 0;
					} else if (StringUtils.isNumeric(account.toString())) {// 转数字
						account = Long.parseLong(account.toString());
					}else{
						account=0;
					}
					// 移除
					json_src.remove("account");
				}
				// 加入
				player.put("account", account);

				// 渠道id
				if (json_src.containsKey("chn")) {
					player.put("cid", json_src.get("chn"));
					json_src.remove("chn");
				}
				// 渠道怅号id
				Object chn_id = 0;
				if (json_src.containsKey("chn_id")) {
					chn_id = json_src.get("chn_id");
					if (null == chn_id || "".equals(chn_id.toString())) {// 空默认0
						chn_id = 0;
					} else if (StringUtils.isNumeric(chn_id.toString())) {// 转数字
						chn_id = Long.parseLong(chn_id.toString());
					}else{//如果非数字，默认=0吧
						chn_id=0;
					}
					// 移除
					json_src.remove("chn_id");
				}
				// 加入
				player.put("cuser", chn_id);

				// 是否支付
				if (json_src.containsKey("is_pay")) {
					Object pay = json_src.get("is_pay");
					if (StringUtils.isNumeric(pay.toString()) && !"".equals(pay.toString())) {
						player.put("pay", Long.parseLong(pay.toString()));
					} else {
						player.put("pay", 0);
					}
					json_src.remove("is_pay");
				} else {
					player.put("pay", 0);
				}
				// 职业
				if (json_src.containsKey("rid") && null != json_src.get("rid")
						&& !"".equals(json_src.get("rid").toString())) {
					Object rid = json_src.get("rid");
					if (StringUtils.isNumeric(rid.toString())) {
						player.put("career", Long.parseLong(rid.toString()));
					} else {
						player.put("career", rid);
					}
					json_src.remove("rid");
				} else {
					player.put("career", 0);
				}

				// 角色id
				Object role_id = 0;
				if (json_src.containsKey("role_id")) {
					role_id = json_src.get("role_id");
					// 转数字
					if (null != role_id && !"".equals(role_id.toString())
							&& StringUtils.isNumeric(role_id.toString())) {
						role_id = Long.parseLong(role_id.toString());
					}
					// 移除
					json_src.remove("role_id");
				}
				// 加入
				player.put("rid", role_id);

				// 角色名
				if (json_src.containsKey("role_name")) {
					player.put("rname", json_src.get("role_name"));
					json_src.remove("role_name");
				}
				// 等级
				if (json_src.containsKey("lv")) {
					Object lv = json_src.get("lv");
					if (StringUtils.isNumeric(lv.toString()) && !"".equals(lv.toString())) {
						player.put("lv", Long.parseLong(lv.toString()));
					} else {
						player.put("lv", 0);
					}
					json_src.remove("lv");
				} else {
					player.put("lv", 0);
				}

				// 战力
				Object power = 0;
				if (json_src.containsKey("power")) {
					power = json_src.get("power");
					if (null == power || "".equals(power.toString())) {// 空默认0
						power = 0;
					} else if (StringUtils.isNumeric(power.toString())) {// 转数字
						power = Long.parseLong(power.toString());
					}
					// 移除
					json_src.remove("power");
				}
				player.put("power", power);

				// vip
				Object vip = 0;
				if (json_src.containsKey("vip")) {
					vip = json_src.get("vip");
					if (null == vip || "".equals(vip.toString())) {// 空默认0
						vip = 0;
					} else if (StringUtils.isNumeric(vip.toString())) {// 转数字
						vip = Long.parseLong(vip.toString());
					}
					// 移除
					json_src.remove("vip");
				}
				player.put("vip", vip);

				// 移除时间字段
				if (json_src.containsKey("time")) {
					json_src.remove("time");
				}
				// 把字段里面的value 数字转数字
				Set<String> key = json_src.keySet();
				//引用转不引用，不然有并发异常
				Iterator it = key.iterator();
				Set<String> key_clone = new HashSet<>();
				while (it.hasNext()) {
					String _k = it.next().toString();
					key_clone.add(_k);
				}
				//
				for (String _k : key_clone) {
					Object _v = json_src.get(_k);
					if (_v == null) {
						json_src.put(_k, "");
						continue;
					}
					if ("".equals(_v)) {
						continue;
					}
					//action不需转
					if ("action".equals(_k)) {
						continue;
					}
					// 时间转换
					if ("ts".equals(_k)) {
						if (StringUtils.isNumeric(_v.toString())) {
							json_src.put("time", Long.parseLong(_v.toString()));
						} else {
							json_src.put("time", 0);
						}
						json_src.remove("ts");
						continue;
					}
					// 如果是数字，转long
					if (StringUtils.isNumeric(_v.toString())) {
						json_src.put(_k, Long.parseLong(_v.toString()));
					}
				}
				// context
				JSONObject context = new JSONObject();
				context.put("player", player);// 共性节点数据
				context.put("data", json_src);// 剩下的就是日志数据
				root.put("context", context);
				// 加入event
				logger.debug("send content...{}", root.toString());
				event.setBody(root.toString().getBytes("utf-8"));
				batch.add(event);
			}
			int size = batch.size();
			int batchSize = client.getBatchSize();
			if (size == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else {
				if (size < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(size);
				client.appendBatch(batch);
			}
			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(size);
		} catch (Throwable t) {
			transaction.rollback();
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof ChannelException) {
				logger.error("Rpc Sink " + getName() + ": Unable to get event from" + " channel " + channel.getName()
						+ ". Exception follows.", t);
				status = Status.BACKOFF;
			} else {
				destroyConnection();
				throw new EventDeliveryException("Failed to send events", t);
			}
		} finally {
			transaction.close();
		}
		return status;
	}

	/**
	 * If this function is called successively without calling
	 * {@see #destroyConnection()}, only the first call has any effect.
	 * 
	 * @throws org.apache.flume.FlumeException
	 *             if an RPC client connection could not be opened
	 */
	private void createConnection() throws FlumeException {

		if (client == null) {
			logger.info("Rpc sink {}: Building RpcClient with hostname: {}, " + "port: {}",
					new Object[] { getName(), hostname, port });
			try {
				resetConnectionFlag = new AtomicBoolean(false);
				client = initializeRpcClient(clientProps);
				Preconditions.checkNotNull(client,
						"Rpc Client could not be " + "initialized. " + getName() + " could not be started");
				sinkCounter.incrementConnectionCreatedCount();
				if (cxnResetInterval > 0) {
					cxnResetExecutor.schedule(new Runnable() {
						@Override
						public void run() {
							resetConnectionFlag.set(true);
						}
					}, cxnResetInterval, TimeUnit.SECONDS);
				}
			} catch (Exception ex) {
				sinkCounter.incrementConnectionFailedCount();
				if (ex instanceof FlumeException) {
					throw (FlumeException) ex;
				} else {
					throw new FlumeException(ex);
				}
			}
			logger.debug("Rpc sink {}: Created RpcClient: {}", getName(), client);
		}

	}

	private void resetConnection() {
		try {
			destroyConnection();
			createConnection();
		} catch (Throwable throwable) {
			// Don't rethrow, else this runnable won't get scheduled again.
			logger.error("Error while trying to expire connection", throwable);
		}
	}

	private void destroyConnection() {
		if (client != null) {
			logger.debug("Rpc sink {} closing Rpc client: {}", getName(), client);
			try {
				client.close();
				sinkCounter.incrementConnectionClosedCount();
			} catch (FlumeException e) {
				sinkCounter.incrementConnectionFailedCount();
				logger.error("Rpc sink " + getName() + ": Attempt to close Rpc " + "client failed. Exception follows.",
						e);
			}
		}

		client = null;
	}

	/**
	 * Ensure the connection exists and is active. If the connection is not
	 * active, destroy it and recreate it.
	 *
	 * @throws org.apache.flume.FlumeException
	 *             If there are errors closing or opening the RPC connection.
	 */
	private void verifyConnection() throws FlumeException {
		if (client == null) {
			createConnection();
		} else if (!client.isActive()) {
			destroyConnection();
			createConnection();
		}
	}

	/**
	 * The start() of RpcSink is more of an optimization that allows connection
	 * to be created before the process() loop is started. In case it so happens
	 * that the start failed, the process() loop will itself attempt to
	 * reconnect as necessary. This is the expected behavior since it is
	 * possible that the downstream source becomes unavailable in the middle of
	 * the process loop and the sink will have to retry the connection again.
	 */
	@Override
	public void start() {
		logger.info("Starting {}...", this);
		sinkCounter.start();
		try {
			createConnection();
		} catch (FlumeException e) {
			logger.warn("Unable to create Rpc client using hostname: " + hostname + ", port: " + port, e);

			/* Try to prevent leaking resources. */
			destroyConnection();
		}

		super.start();

		logger.info("Rpc sink {} started.", getName());
	}

	@Override
	public void stop() {
		logger.info("Rpc sink {} stopping...", getName());

		destroyConnection();
		cxnResetExecutor.shutdown();
		try {
			if (cxnResetExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
				cxnResetExecutor.shutdownNow();
			}
		} catch (Exception ex) {
			logger.error("Interrupted while waiting for connection reset executor to shut down");
		}
		sinkCounter.stop();
		super.stop();

		logger.info("Rpc sink {} stopped. Metrics: {}", getName(), sinkCounter);
	}

	@Override
	public String toString() {
		return "RpcSink " + getName() + " { host: " + hostname + ", port: " + port + " }";
	}

	@VisibleForTesting
	RpcClient getUnderlyingClient() {
		return client;
	}
}
