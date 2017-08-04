package com.flume.dome.mysink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.dianping.cat.Cat;
import com.flume.dome.xutils.ConverData;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class DBsqlSink extends AbstractSink implements Configurable {

	private Logger LOG = LoggerFactory.getLogger(DBsqlSink.class);
	private String hostname;
	private String port;
	private String databaseName;
	private String tableName;
	private String user;
	private String password;
	private PreparedStatement preparedStatement;
	private PreparedStatement preparedStatement2;
	private Statement statement;
	private Connection conn;
	private Integer serverId;
	private int batchSize;// 批处理数量
	private String josnTo = "true";// 是否转换json
	private SinkCounter sinkCounter;
	private final CounterGroup counterGroup = new CounterGroup();

	public DBsqlSink() {
		LOG.info("MysqlSink start...");
	}

	public Status process() throws EventDeliveryException {
		LOG.debug("processing...");
		Status status = Status.READY;

		com.dianping.cat.message.Transaction t = Cat.newTransaction("Exec", "flume-" + serverId);
		// cat监控记录一事件
		Cat.logEvent("Exec.event", serverId.toString(), com.dianping.cat.message.Event.SUCCESS, "sid=" + serverId);
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event;
		String content;
		// 数据集合
		List<JSONObject> actions = Lists.newArrayList();
		transaction.begin();
		try {
			int count;
			for (count = 0; count < batchSize; ++count) {
				// for (int i = 0; i < batchSize; i++) {
				event = channel.take();// 从通道中获取数据
				// 一条日志
				Cat.logMetricForCount("flume-db-log-take");
				if (event == null) {
					// 没记录的数据
					Cat.logMetricForCount("flume-db-log-event-null");
					break;
				}
				content = new String(event.getBody());
				// Log.info("josnTo {},src content:{}", josnTo, content);
				if (josnTo != null && "true".equals(josnTo)) {
					// 把文本按行分隔，把kv转json
					List<JSONObject> action = ConverData.conver(content);
					actions.addAll(action);
				} else {
					// 字符串转json
					List<JSONObject> action = ConverData.converStr(content);
					actions.addAll(action);
				}
			}

			// 检查批量是否合法
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
			}

			// 偿试event事件发送
			sinkCounter.addToEventDrainAttemptCount(count);
			int commit_count = 0;
			if (actions != null && actions.size() > 0) {
				preparedStatement.clearBatch();
				preparedStatement2.clearBatch();
				for (JSONObject json : actions) {
					Log.info("log inserint json:{}", json.toString());
					if (StringUtils.isBlank(json.toJSONString()) || "{}".equals(json.toString())) {
						// 没记录的数据
						Cat.logMetricForCount("flume-db-log-jsos-{}");
						continue;
					}
					// 如果是属性，则插入另一张表
					if ("attrs".equals(json.getString("file"))) {
						// 对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
						if (serverId == null) {
							preparedStatement2.setInt(1, -2);
						} else {
							preparedStatement2.setInt(1, Integer.valueOf(serverId));
						}
						preparedStatement2.setString(2, json.toString());
						preparedStatement2.setString(3, json.getString("time"));
						preparedStatement2.setString(4, json.getString("file"));
						preparedStatement2.setLong(5, json.getLongValue("time_log"));
						preparedStatement2.setLong(6, json.getLongValue("uuid"));
						preparedStatement2.addBatch();
						commit_count++;
					} else {
						// 对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
						if (serverId == null) {
							preparedStatement.setInt(1, -2);
						} else {
							preparedStatement.setInt(1, Integer.valueOf(serverId));
						}
						preparedStatement.setString(2, json.toString());
						preparedStatement.setString(3, json.getString("time"));
						preparedStatement.setString(4, json.getString("file"));
						preparedStatement.setLong(5, json.getLongValue("time_log"));
						preparedStatement.setLong(6, json.getLongValue("uuid"));
						preparedStatement.addBatch();
						commit_count++;
					}
				}
				// 提交那个sql
				// if("1".equals(com_way)){
				preparedStatement.executeBatch();
				// }
				// if("2".equals(com_way)){
				preparedStatement2.executeBatch();
				// }
				conn.commit();
				transaction.commit();
				// event发送成功
				sinkCounter.addToEventDrainSuccessCount(count);
				counterGroup.incrementAndGet("transaction.success");
			} else {
				conn.rollback();
				transaction.rollback();
				counterGroup.incrementAndGet("transaction.rollback");

			}
			// 实际批量提交了多少
			Cat.logMetricForSum("flume-db-commit_count", commit_count);
			// 监控提交状态
			t.setStatus(com.dianping.cat.message.Transaction.SUCCESS);
		} catch (Throwable ex) {
			// 监控提交状态
			t.setStatus(ex);
			try {
				transaction.rollback();
				counterGroup.incrementAndGet("transaction.rollback");
			} catch (Exception ex2) {
				LOG.error("Exception in rollback. Rollback might not have been successful.", ex2);
			}

			if (ex instanceof Error || ex instanceof RuntimeException) {
				LOG.error("Failed to commit transaction. Transaction rolled back.", ex);
				Throwables.propagate(ex);
			} else {
				LOG.error("Failed to commit transaction. Transaction rolled back.", ex);
				throw new EventDeliveryException("Failed to commit transaction. Transaction rolled back.", ex);
			}

		} finally {
			t.complete();// 监控提交状态
			transaction.close();
			// conn.close();
		}
		return status;
	}

	public void configure(Context context) {
		hostname = context.getString("hostname");
		Preconditions.checkNotNull(hostname, "hostname must be set!!");
		// port = context.getString("port");
		// Preconditions.checkNotNull(port, "port must be set!!");
		databaseName = context.getString("databaseName");
		Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
		tableName = context.getString("tableName");
		Preconditions.checkNotNull(tableName, "tableName must be set!!");
		user = context.getString("user");
		Preconditions.checkNotNull(user, "user must be set!!");
		password = context.getString("password");
		Preconditions.checkNotNull(password, "password must be set!!");
		batchSize = context.getInteger("batchSize", 100);
		serverId = context.getInteger("serverId");
		josnTo = context.getString("josnTo");
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
		Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");

	}

	@Override
	public synchronized void start() {
		super.start();

		sinkCounter.start();

		String url = hostname + "/" + databaseName;
		Log.info("mysql start url:{}", url);
		// 调用DriverManager对象的getConnection()方法，获得一个Connection对象
		try {

			sinkCounter.incrementConnectionCreatedCount();
			Class.forName("org.postgresql.Driver"); // 调用Class.forName()方法加载驱动程序org.postgresql
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			// 创建一个Statement对象
			// String sql="insert into "+ tableName + " (context,time) values
			// (?,?)";
			String sql = "INSERT INTO " + tableName
					+ " (server_id,cont,time,file,time_log,uuid) VALUES (?,cast(? AS json),cast(? AS timestamp),?,?,?)";
			preparedStatement = conn.prepareStatement(sql);

			String sql2 = "INSERT INTO " + tableName
					+ "_attrs (server_id,cont,time,file,time_log,uuid) VALUES (?,cast(? AS json),cast(? AS timestamp),?,?,?)";
			preparedStatement2 = conn.prepareStatement(sql2);
		} catch (SQLException e) {
			sinkCounter.incrementConnectionFailedCount();
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// sinkCounter.incrementConnectionFailedCount();
			sinkCounter.incrementConnectionClosedCount();// 连接关闭
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void stop() {
		super.stop();
		sinkCounter.incrementConnectionClosedCount();
		sinkCounter.stop();
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

}
