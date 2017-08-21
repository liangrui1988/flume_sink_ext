package com.flume.dome.interceptor;

import static com.flume.dome.interceptor.ConverDataInterceptor.Constants.*;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;
import com.flume.dome.xutils.ConverDataObject;

/**
 * 自定义拦截器
 * 
 * @author rui
 * @data 2017、08、18
 */
public class ConverDataInterceptor implements Interceptor {
	private static Logger LOG = LoggerFactory.getLogger(ConverDataInterceptor.class);
	private final boolean preserveExisting;

	/**
	 * 是否转kv转换json处理
	 */
	private final boolean jsonConver;
	/**
	 * 是否开启过虑
	 */
	private final boolean consider;

	/**
	 * Only {@link TimestampInterceptor.Builder} can build me
	 */
	private ConverDataInterceptor(boolean preserveExisting, boolean jsonConver, boolean consider) {
		this.preserveExisting = preserveExisting;
		this.jsonConver = jsonConver;
		this.consider = consider;
	}

	public void initialize() {

	}

	public Event intercept(Event event) {
		JSONObject action = null;
		try {
			// 转换
			String content = new String(event.getBody(), "utf-8");
			LOG.debug("拦截器 处理前event.getBody()数据 content:{}", content);
			LOG.debug("拦截器 处理前event.getHeaders() 数据 Headers:{}", event.getHeaders());

			if (StringUtils.isBlank(content)) {
				return null;
			}
			if (jsonConver) {
				// 把文本按行分隔，把kv转json
				action = ConverDataObject.conver(content, consider);
			} else {
				// 字符串转json
				action = ConverDataObject.converStr(content);
			}
		} catch (ParseException | UnsupportedEncodingException e) {
			LOG.error("转换数据错误码," + e.getMessage());
		}
		// 去除空字符
		if (action == null || StringUtils.isBlank(action.toJSONString()) || "{}".equals(action.toJSONString())) {
			return null;
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
		action.put("server_id", sid);
		// 加入event
		event.setBody(action.toString().getBytes());
		LOG.debug("拦截器  处理后的数据 content:{}", action.toString());
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		List<Event> list = new LinkedList<>();
		// 去除空
		for (Event event : events) {
			if (event == null || event.getBody() == null) {
				continue;
			}
			Event e = intercept(event);
			if (e != null && e.getBody() != null) {
				list.add(e);
			}
		}
		return list;
	}
	public void close() {
	}

	/**
	 * Builder which builds new instances of the TimestampInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {
		private boolean preserveExisting = PRESERVE_DFLT;
		private boolean jsonConver = JSON_CONVER_DFLT;
		private boolean consider = CONSIDER_DFLT;
		@Override
		public Interceptor build() {
			return new ConverDataInterceptor(preserveExisting, jsonConver, consider);
		}
		@Override
		public void configure(Context context) {
			// preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
			jsonConver = context.getBoolean(JSONCONVER, JSON_CONVER_DFLT);
			consider = context.getBoolean(CONSIDER, CONSIDER_DFLT);
		}
	}

	public static class Constants {
		// public static String TIMESTAMP = "timestamp";
		// public static String PRESERVE = "preserveExisting";
		public static String JSONCONVER = "jsonConver";
		public static String CONSIDER = "consider";
		public static boolean PRESERVE_DFLT = false;
		public static boolean JSON_CONVER_DFLT = false;// 是否转json
		public static boolean CONSIDER_DFLT = true;// 是否过虑数据

	}
}
