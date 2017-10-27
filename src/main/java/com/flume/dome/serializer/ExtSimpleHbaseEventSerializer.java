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

package com.flume.dome.serializer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.flume.sink.hbase.SimpleRowKeyGenerator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * A simple serializer that returns puts from an event, by writing the event
 * body into it. The headers are discarded. It also updates a row in hbase which
 * acts as an event counter.
 * <p>
 * Takes optional parameters:
 * <p>
 * <tt>rowPrefix:</tt> The prefix to be used. Default: <i>default</i>
 * <p>
 * <tt>incrementRow</tt> The row to increment. Default: <i>incRow</i>
 * <p>
 * <tt>suffix:</tt> <i>uuid/random/timestamp.</i>Default: <i>uuid</i>
 * <p>
 * <p>
 * Mandatory parameters:
 * <p>
 * <tt>cf:</tt>Column family.
 * <p>
 * Components that have no defaults and will not be used if null:
 * <tt>payloadColumn:</tt> Which column to put payload in. If it is null, event
 * data will not be written.
 * <p>
 * <tt>incColumn:</tt> Which column to increment. Null means no column is
 * incremented.
 */
public class ExtSimpleHbaseEventSerializer implements HbaseEventSerializer {
	private static Logger LOG = LoggerFactory.getLogger(ExtSimpleHbaseEventSerializer.class);

	private String rowPrefix;
	private byte[] incrementRow;
	private byte[] cf;
	private byte[] plCol;
	private byte[] incCol;
	private KeyType keyType;
	private byte[] payload;
	// 常量名
	private static final String SERVER_ID_CONST = "server_id";
	// private String server_id;

	public ExtSimpleHbaseEventSerializer() {
	}

	public static void main(String[] args) {
		System.out.println(KeyType.TS);
	}

	@Override
	public void configure(Context context) {
		Log.debug("context.getParameters:{}", context.getParameters());
		// rowPrefix = context.getString("rowPrefix", "default");
		incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
		String suffix = context.getString("suffix", "uuid");
		String payloadColumn = context.getString("payloadColumn", "pCol");
		String incColumn = context.getString("incrementColumn", "iCol");
		if (payloadColumn != null && !payloadColumn.isEmpty()) {
			if (suffix.equals("timestamp")) {
				keyType = KeyType.TS;
			} else if (suffix.equals("random")) {
				keyType = KeyType.RANDOM;
			} else if (suffix.equals("nano")) {
				keyType = KeyType.TSNANO;
			} else {
				keyType = KeyType.UUID;
			}
			plCol = payloadColumn.getBytes(Charsets.UTF_8);
		}
		if (incColumn != null && !incColumn.isEmpty()) {
			incCol = incColumn.getBytes(Charsets.UTF_8);
		}
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public void initialize(Event event, byte[] cf) {
		this.payload = event.getBody();
		this.cf = cf;
		// 获取服务器id
		Map<String, String> map = event.getHeaders();
		if (map != null && map.size() > 0 && map.containsKey(SERVER_ID_CONST)) {
			// this.server_id=map.get(SERVER_ID_CONST);
			// 服务器id作为rowkey前辍
			this.rowPrefix = map.get(SERVER_ID_CONST);
		} else {
			this.rowPrefix = "def";
		}
	}

	@Override
	public List<Row> getActions() throws FlumeException {
		List<Row> actions = new LinkedList<Row>();
		if (plCol != null) {
			byte[] rowKey;
			try {
				if (keyType == KeyType.TS) {
					rowKey = SimpleRowKeyGenerator.getTimestampKey(rowPrefix);
				} else if (keyType == KeyType.RANDOM) {
					rowKey = SimpleRowKeyGenerator.getRandomKey(rowPrefix);
				} else if (keyType == KeyType.TSNANO) {
					rowKey = SimpleRowKeyGenerator.getNanoTimestampKey(rowPrefix);
				} else {
					rowKey = SimpleRowKeyGenerator.getUUIDKey(rowPrefix);
				}

				// 这里改成拆分后的k=v存储
				String content = new String(payload, "utf-8");
				LOG.debug("content>>>:{}", content);
				// 按原日志拆分后 存回集合
				String[] kv1 = content.split("`");
				for (String str : kv1) {
					String[] kv2 = str.split("=");
					if (kv2 != null && kv2.length == 2) {
						byte[] payloadColumn_conev = kv2[0].getBytes(Charsets.UTF_8);
						// long double值转换，如果需要条件查询时会用到比较，不然是以字符串作为比较
//						byte[] payload_conevt = null;
//						String v2 = kv2[1];
//						if (StringUtils.isNumeric(v2)) {
//							if (v2.contains(".")) {
//								// double
//								Double l_v = Double.valueOf(kv2[1]);
//								payload_conevt = Bytes.toBytes(l_v);
//							} else {
//								Long l_v = Long.valueOf(kv2[1]);
//								payload_conevt = Bytes.toBytes(l_v);
//							}
//
//						}
						byte[] payload_conev = kv2[1].getBytes(Charsets.UTF_8);
						LOG.debug("rowKey>:{},payloadColumn>:{},payload>:{}", new Object[] { new String(rowKey),
								new String(payloadColumn_conev), new String(payload_conev) });
						Put put = new Put(rowKey);
						put.addColumn(cf, payloadColumn_conev, payload_conev);
						actions.add(put);
					}
				}
				// 服务器
				Put put = new Put(rowKey);
				put.addColumn(cf, SERVER_ID_CONST.getBytes(Charsets.UTF_8), rowPrefix.getBytes(Charsets.UTF_8));
				actions.add(put);

				// Put put = new Put(rowKey);
				// put.add(cf, plCol, payload);
				// actions.add(put);
			} catch (Exception e) {
				throw new FlumeException("Could not get row key!", e);
			}

		}
		return actions;
	}

	@Override
	public List<Increment> getIncrements() {
		List<Increment> incs = new LinkedList<Increment>();
		if (incCol != null) {
			Increment inc = new Increment(incrementRow);
			inc.addColumn(cf, incCol, 1);
			incs.add(inc);
		}
		return incs;
	}

	@Override
	public void close() {
	}

	public enum KeyType {
		UUID, RANDOM, TS, TSNANO;
	}

}
