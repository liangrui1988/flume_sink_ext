package com.flume.dome.mysink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
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
	private Connection conn;
	private Integer serverId;
	private int batchSize;// 批处理数量

	public DBsqlSink() {
		LOG.info("MysqlSink start...");
	}

	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event;
		String content;

		// 数据集合
		List<JSONObject> actions = Lists.newArrayList();
		transaction.begin();
		try {
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();// 从通道中获取数据
				if (event != null) {
					content = new String(event.getBody());
					// 把文本返换行分隔，并转json
					actions = ConverData.conver(content);
					// actions.add(content);
				} else {
					result = Status.BACKOFF;
					break;
				}
			}
			if (actions.size() > 0) {
				preparedStatement.clearBatch();
				for (JSONObject json : actions) {
					// String sql = "INSERT INTO " + tableName +"
					// (server_id,cont,time,file) VALUES (?,cast(? AS
					// json),cast(? AS timestamp),?)";
					Log.info("log inserint json:{}", json.toString());
					// 对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
					// preparedStatement.setString(1, temp);
					if (serverId == null) {
						preparedStatement.setInt(1, -2);
					} else {
						preparedStatement.setInt(1, Integer.valueOf(serverId));
					}
					preparedStatement.setString(2, json.toString());
					preparedStatement.setString(3, json.getString("time"));
					preparedStatement.setString(4, json.getString("file"));
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();
				conn.commit();
			}
			transaction.commit();
		} catch (Throwable e) {
			try {
				transaction.rollback();
			} catch (Exception e2) {
				LOG.error("Exception in rollback. Rollback might not have been" + "successful.", e2);
			}
			LOG.error("Failed to commit transaction." + "Transaction rolled back.", e);
			Throwables.propagate(e);
		} finally {
			transaction.close();
		}
		return result;
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

		Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");

	}

	@Override
	public synchronized void start() {
		super.start();
		try {
			// 调用Class.forName()方法加载驱动程序org.postgresql
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		// String url = "jdbc:mysql://" + hostname + ":" + port + "/"
		// + databaseName;
		String url = hostname + "/" + databaseName;
		Log.info("mysql start url:{}", url);
		// 调用DriverManager对象的getConnection()方法，获得一个Connection对象
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			// 创建一个Statement对象
			// String sql="insert into "+ tableName + " (context,time) values
			// (?,?)";
			String sql = "INSERT INTO " + tableName
					+ " (server_id,cont,time,file) VALUES (?,cast(? AS json),cast(? AS timestamp),?)";
			preparedStatement = conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public synchronized void stop() {
		super.stop();
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
