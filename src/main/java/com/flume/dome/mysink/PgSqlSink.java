package com.flume.dome.mysink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class PgSqlSink extends AbstractSink implements Configurable {
	private Logger LOG = LoggerFactory.getLogger(PgSqlSink.class);
	private String hostname;
	// private String port;
	private String databaseName;
	private String tableName;
	private String user;
	private String password;
	private PreparedStatement preparedStatement;
	// private Statement statement;
	private Connection conn;
	// private Integer serverId;
	private int batchSize;// 批处理数量
	// private String josnTo = "true";// 是否转换json
	private SinkCounter sinkCounter;
	private final CounterGroup counterGroup = new CounterGroup();

	public PgSqlSink() {
		LOG.info("PGsqlSink start...");
	}

	public Status process() throws EventDeliveryException {
		LOG.debug("processing...");
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		// Event event;
		// 数据集合
		List<JSONObject> actions = Lists.newArrayList();
		transaction.begin();
		try {
			//
			long i = 0;
			for (; i < batchSize; i++) {
				Event event = channel.take();
				LOG.debug("event...{}", event);
				if (event == null) {
					if (i == 0) {
						// No events found, request back-off semantics from
						// runner
						status = Status.BACKOFF;
						sinkCounter.incrementBatchEmptyCount();
					} else {
						sinkCounter.incrementBatchUnderflowCount();
					}

					break;
				} else {
					String content = new String(event.getBody(), "utf-8");
					LOG.debug("content...{}", content);
					JSONObject jsonObj = (JSONObject) JSONObject.parse(content);
					actions.add(jsonObj);
				}
			}
			if (i == batchSize) {
				sinkCounter.incrementBatchCompleteCount();
			}
			sinkCounter.addToEventDrainAttemptCount(i);
			// 提交,写入数据库
			iniserInto(actions, transaction);
			// event发送成功
			sinkCounter.addToEventDrainSuccessCount(actions.size());
			counterGroup.incrementAndGet("transaction.success");
		} catch (Throwable ex) {
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
			transaction.close();
			// try {
			// if (conn != null) {
			// conn.close();
			// }
			// } catch (SQLException e) {
			// e.printStackTrace();
			// }
		}
		return status;
	}

	/**
	 * 写入数据库逻辑
	 * 
	 * @param actions
	 * @param transaction
	 * @throws SQLException
	 */
	public void iniserInto(List<JSONObject> actions, Transaction transaction) throws SQLException {
		preparedStatement.clearBatch();
		for (JSONObject json : actions) {
			Log.debug("log inserint json:{},actions size:{}", json.toString(), actions.size());
			if (StringUtils.isBlank(json.toJSONString()) || "{}".equals(json.toString())) {
				continue;
			}
			// 如果是属性，则插入另一张表
			// 对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
			/// @file,/,/@role_id,/@server_id,/@ts,/@time
			if (json.containsKey("server_id")) {
				preparedStatement.setInt(1, json.getIntValue("server_id"));
			} else {
				preparedStatement.setInt(1, -2);
			}
			preparedStatement.setString(2, json.toString());
			if (json.containsKey("time")) {
				preparedStatement.setString(3, json.getString("time"));
			} else {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				preparedStatement.setString(3, df.format(new Date()));
			}
			preparedStatement.setString(4, json.getString("file"));

			if (json.containsKey("ts")) {
				preparedStatement.setLong(5, json.getLongValue("ts"));
			} else {
				preparedStatement.setLong(5, 0);
			}
			if (json.containsKey("role_id")) {
				preparedStatement.setLong(6, json.getLongValue("role_id"));
			} else {
				preparedStatement.setLong(6, 0);
			}
			preparedStatement.addBatch();
			// commit_count++;
		}
		preparedStatement.executeBatch();
		conn.commit();
		transaction.commit();
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
		// serverId = context.getInteger("serverId");
		// josnTo = context.getString("josnTo");
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
		Log.info("pgSql start url:{}", url);
		// 调用DriverManager对象的getConnection()方法，获得一个Connection对象
		try {
			sinkCounter.incrementConnectionCreatedCount();
			Class.forName("org.postgresql.Driver"); // 调用Class.forName()方法加载驱动程序org.postgresql
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			// 创建一个Statement对象
			// String sql="insert into "+ tableName + " (context,time) values
			// (?,?)";
			// String sql = "INSERT INTO " + tableName
			// + " (server_id,cont,time,file,time_log,uuid) VALUES (?,cast(? AS
			// json),cast(? AS timestamp),?,?,?)";

			String sql = "INSERT INTO " + tableName
					+ " (server_id,cont,time,file,time_log,uuid) VALUES (?,cast(? AS jsonb),cast(? AS timestamp),?,?,?)";
			preparedStatement = conn.prepareStatement(sql);
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
