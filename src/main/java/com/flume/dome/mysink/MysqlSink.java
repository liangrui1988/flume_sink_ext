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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class MysqlSink extends AbstractSink implements Configurable {

	private Logger LOG = LoggerFactory.getLogger(MysqlSink.class);
	private String hostname;
	private String port;
	private String databaseName;
	private String tableName;
	private String user;
	private String password;
	private PreparedStatement preparedStatement;
	private Connection conn;
	private int batchSize;// 批处理数量

	public MysqlSink() {
		LOG.info("MysqlSink start...");
	}

	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event;
		String content;

		// 数据集合
		List<String> actions = Lists.newArrayList();
		transaction.begin();
		try {
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();// 从通道中获取数据
				if (event != null) {
					content = new String(event.getBody());
					actions.add(content);
				} else {
					result = Status.BACKOFF;
					break;
				}
			}
			if (actions.size() > 0) {
				preparedStatement.clearBatch();
				for (String temp : actions) {
					Log.info("actions temp:{}", temp);
					// 对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
					preparedStatement.setString(1, temp);
					preparedStatement.setLong(2, System.currentTimeMillis());
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
				LOG.error("Exception in rollback. Rollback might not have been"
						+ "successful.", e2);
			}
			LOG.error("Failed to commit transaction."
					+ "Transaction rolled back.", e);
			Throwables.propagate(e);
		} finally {
			transaction.close();
		}
		return result;
	}

	public void configure(Context context) {
		hostname = context.getString("hostname");
		Preconditions.checkNotNull(hostname, "hostname must be set!!");
		port = context.getString("port");
		Preconditions.checkNotNull(port, "port must be set!!");
		databaseName = context.getString("databaseName");
		Preconditions.checkNotNull(databaseName, "databaseName must be set!!");
		tableName = context.getString("tableName");
		Preconditions.checkNotNull(tableName, "tableName must be set!!");
		user = context.getString("user");
		Preconditions.checkNotNull(user, "user must be set!!");
		password = context.getString("password");
		Preconditions.checkNotNull(password, "password must be set!!");
		batchSize = context.getInteger("batchSize", 100);
		Preconditions.checkNotNull(batchSize > 0,
				"batchSize must be a positive number!!");

	}

	@Override
	public synchronized void start() {
		super.start();
		try {
			// 调用Class.forName()方法加载驱动程序
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		String url = "jdbc:mysql://" + hostname + ":" + port + "/"
				+ databaseName;
		Log.info("mysql start url:{}", url);
		// 调用DriverManager对象的getConnection()方法，获得一个Connection对象
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			// 创建一个Statement对象
			preparedStatement = conn.prepareStatement("insert into "
					+ tableName + " (context,time) values (?,?)");
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
