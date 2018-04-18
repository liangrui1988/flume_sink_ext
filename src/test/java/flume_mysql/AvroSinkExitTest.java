package flume_mysql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;

public class AvroSinkExitTest {

	public static void main(String[] args) {
		 String		 body="{\"summon\":\"1\",\"remain_gold\":\"870\",\"screen\":\"1024*768\",\"lv\":\"240\",\"type\":\"offline\",\"rid\":\"1\",\"login\":\"1523512570\",\"gem_total\":\"0\",\"used_len\":\"12\",\"platform\":\"win\",\"duration\":\"283\",\"offline\":\"1523512853\",\"file\":\"player_time\",\"ps_1000\":\"60\",\"ps_1001\":\"60\",\"role_id\":\"23000079\",\"ps_1002\":\"0\",\"ps_1200\":\"1\",\"ps_1003\":\"1\",\"ps_1004\":\"0\",\"model\":\"Windows\",\"power\":\"24925\",\"vip\":\"0\",\"net\":\"2\",\"remain_silver\":\"175\",\"bag_len\":\"50\",\"player\":\"20\",\"pay_sum\":\"0\",\"os\":\"win\",\"dg_pos\":\"3356.0,1825.0\",\"fury\":\"0\",\"ip\":\"192.168.20.131\",\"chn\":\"uc\",\"ps_1005\":\"0\",\"ps_1006\":\"0\",\"remain_copper\":\"3269384\",\"server_id\":23,\"chn_id\":\"provider_passport\",\"role_name\":\"杭天川\",\"dgid\":\"10401\",\"grahics\":\"1\",\"guild_id\":\"9023\",\"time\":\"2018-04-13 13:55:10\",\"is_pay\":\"0\",\"birth_time\":\"1522270918\",\"account\":\"wgl03\",\"ts\":\"1523512853261\",\"gem_lv\":\"0\"}";

//		String body = "{\"time\":\"2018-04-13 13:55:10\",\"ts\":\"1523512853261\",\"gem_lv\":\"0\"}";

		JSONObject json_src = (JSONObject) JSONObject.parse(body);

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
			} else {
				account = 0;
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
			} else {// 如果非数字，默认=0吧
				chn_id = 0;
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
		if (json_src.containsKey("rid") && null != json_src.get("rid") && !"".equals(json_src.get("rid").toString())) {
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
			if (null != role_id && !"".equals(role_id.toString()) && StringUtils.isNumeric(role_id.toString())) {
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

		Iterator it = key.iterator();
		Set<String> key_clone = new HashSet<>();
		while (it.hasNext()) {
			String _k = it.next().toString();
			key_clone.add(_k);
		}

		for (String _k : key_clone) {
			System.out.println(_k);
			Object _v = json_src.get(_k);
			if (_v == null) {
				json_src.put(_k, "");
				continue;
			}
			if ("".equals(_v)) {
				continue;
			}
			// action不需转
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
		//

		// context
		JSONObject context = new JSONObject();
		context.put("player", player);// 共性节点数据
		context.put("data", json_src);// 剩下的就是日志数据
		root.put("context", context);
		// 加入event
		System.out.println("send content...{}" + root.toString());
	}
}
