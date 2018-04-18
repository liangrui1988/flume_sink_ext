package flume_mysql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;

public class AvroSinkExitTest2 {

	public static void main(String[] args) {

		String arrst = "{\"lv\": \"85\", \"os\": \"android\", \"ts\": \"1524041533196\", \"chn\": \"eagle_248\", \"rid\": \"1\", \"vip\": \"0\", \"file\": \"act\", \"time\": \"2018-04-18 16:52:12\", \"type\": \"finish\", \"power\": \"26382\", \"total\": \"54\", \"act_id\": \"59\", \"chn_id\": \"1112356\", \"is_pay\": \"0\", \"account\": \"2725\", \"role_id\": \"65000877\", \"role_name\": \"岳磬\", \"server_id\": 1065, \"add_liveness\": \"10\"}";
		String arrst1 = "	{\"lv\": \"1\", \"os\": \"unkown\", \"ts\": \"1524039930886\", \"chn\": \"eagle_248\", \"rid\": \"1\", \"vip\": \"0\", \"file\": \"born\", \"time\": \"2018-04-18 16:25:30\", \"type\": \"born\", \"power\": \"666\", \"chn_id\": \"1112984\", \"is_pay\": \"0\", \"account\": \"2799\", \"role_id\": \"65000898\", \"role_name\": \"惠百川\", \"server_id\": 1065}";
		String arrst2 = "{\"lv\": \"90\", \"os\": \"android\", \"ts\": \"1524041793447\", \"chn\": \"eagle_248\", \"num\": \"1\", \"rid\": \"3\", \"vip\": \"0\", \"dgid\": \"2005\", \"file\": \"dungeon\", \"mark\": \"1\", \"time\": \"2018-04-18 16:56:33\", \"type\": \"result\", \"power\": \"38087\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"result\": \"1\", \"account\": \"10\", \"in_time\": \"35631\", \"is_team\": \"1\", \"need_lv\": \"1\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst3 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524041973474\", \"chn\": \"eagle_248\", \"gid\": \"303316\", \"pos\": \"6\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"equip\", \"time\": \"2018-04-18 16:59:33\", \"type\": \"strength\", \"power\": \"38301\", \"to_lv\": \"14\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"account\": \"10\", \"from_lv\": \"13\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst4 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524041943351\", \"chn\": \"eagle_248\", \"num\": \"3833500\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"exp\", \"time\": \"2018-04-18 16:59:02\", \"type\": \"add\", \"power\": \"38267\", \"action\": \"main\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"system\": \"quest\", \"account\": \"10\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065, \"accumulate\": \"1\"}";
		String arrst5 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524042035255\", \"chn\": \"eagle_248\", \"num\": \"499365\", \"oid\": \"26351065\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"exp_jade\", \"time\": \"2018-04-18 17:00:34\", \"type\": \"add_exp\", \"power\": \"38335\", \"action\": \"1015\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"system\": \"exp_jade\", \"account\": \"10\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065, \"accumulate\": \"137\"}";
		String arrst6 = "{\"lv\": \"-1\", \"ts\": \"1524040205487\", \"rid\": \"0\", \"file\": \"gold_thief\", \"time\": \"2018-04-18 16:30:05\", \"type\": \"gold_thief\", \"killer\": \"65000882\", \"max_hp\": \"10\", \"mon_id\": \"11000\", \"hit_num\": \"10\", \"role_id\": \"-1\", \"hurt_num\": \"1\", \"born_time\": \"1523948998\", \"dead_time\": \"1524040205\", \"mon_index\": \"201\", \"role_name\": \"-1\", \"server_id\": 1065, \"mon_seq_index\": \"14\"}";
		String arrst7 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524041984383\", \"chn\": \"eagle_248\", \"gid\": \"1044\", \"num\": \"356\", \"oid\": \"26067065\", \"rid\": \"3\", \"vip\": \"0\", \"bind\": \"1\", \"file\": \"goods\", \"name\": \"中型内力药水\", \"time\": \"2018-04-18 16:59:43\", \"type\": \"source\", \"power\": \"38335\", \"action\": \"use\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"system\": \"goods\", \"account\": \"10\", \"role_id\": \"65000879\", \"bag_type\": \"1\", \"role_name\": \"越伊\", \"server_id\": 1065, \"goods_type\": \"1\"}";
		String arrst8 = "{\"lv\": \"89\", \"os\": \"android\", \"ts\": \"1524041694915\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"guide\", \"time\": \"2018-04-18 16:54:54\", \"type\": \"mark\", \"power\": \"33953\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"account\": \"10\", \"role_id\": \"65000879\", \"guide_id\": \"1032\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst9 = "{\"lv\": \"-1\", \"ts\": \"1524039860723\", \"rid\": \"0\", \"file\": \"guild\", \"time\": \"2018-04-18 16:24:20\", \"type\": \"join\", \"join_lv\": \"50\", \"role_id\": \"-1\", \"guild_id\": \"1065\", \"join_uuid\": \"65000879\", \"role_name\": \"-1\", \"server_id\": 1065, \"guild_name\": \"6个6\", \"join_power\": \"14365\"}";
		String arrst10 = "{\"lv\": \"-1\", \"ts\": \"1523799001447\", \"rid\": \"0\", \"file\": \"guild_league\", \"time\": \"2018-04-15 21:30:01\", \"type\": \"league_rank\", \"leader\": \"65000080\", \"role_id\": \"-1\", \"guild_id\": \"1065\", \"role_name\": \"-1\", \"server_id\": 1065, \"guild_name\": \"6个6\", \"level_rank\": \"1\", \"battle_power\": \"223606\", \"league_level\": \"1\"}";
		String arrst11 = "{\"lv\": \"110\", \"os\": \"android\", \"ts\": \"1523432077453\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"hang\", \"time\": \"2018-04-11 15:34:37\", \"type\": \"hang_reward\", \"power\": \"78117\", \"chn_id\": \"934482\", \"is_pay\": \"0\", \"account\": \"1102\", \"role_id\": \"65000079\", \"swallow\": \"[]\", \"base_exp\": \"486000\", \"show_exp\": \"4879440\", \"buff_rate\": \"5000\", \"buff_time\": \"720\", \"role_name\": \"狂扁小西瓜\", \"server_id\": 1065, \"total_num\": \"808\", \"extra_mult\": \"3.04\", \"show_equip\": \"[]\", \"show_money\": \"[{11,188}]\", \"total_time\": \"720\", \"extra_goods\": \"[{303204,3},{303205,3},{1081,21}]\", \"extra_money\": \"[{11,571}]\", \"stream_goods\": \"[{303204,1},{303205,1},{1081,7}]\", \"stream_money\": \"[{11,188}]\", \"total_drop_ids\": \"[{131,808}]\", \"rare_reward_ids\": \"[]\", \"stream_drop_ids\": \"[{131,200}]\"}";
		String arrst12 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524041873750\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"lv_up\", \"time\": \"2018-04-18 16:57:53\", \"type\": \"lv_up\", \"power\": \"38167\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"new_lv\": \"92\", \"old_lv\": \"91\", \"account\": \"10\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst13 = "{\"lv\": \"1\", \"os\": \"android\", \"ts\": \"1524039333301\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"mail\", \"from\": \"游戏运营团队\", \"time\": \"2018-04-18 16:15:32\", \"type\": \"add\", \"power\": \"666\", \"action\": \"gm\", \"chn_id\": \"1112833\", \"is_pay\": \"0\", \"system\": \"admin\", \"account\": \"2780\", \"mail_id\": \"1018065\", \"role_id\": \"65000897\", \"type_id\": \"0\", \"role_name\": \"束伟泽\", \"server_id\": 1065, \"attach_len\": \"1\"}";
		String arrst14 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524041996544\", \"chn\": \"eagle_248\", \"cur\": \"20064\", \"gid\": \"11\", \"num\": \"186\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"money\", \"time\": \"2018-04-18 16:59:56\", \"type\": \"add\", \"power\": \"38335\", \"action\": \"1015\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"system\": \"dungeon_reward\", \"account\": \"10\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst15 = "{\"lv\": \"-1\", \"ts\": \"1522568785617\", \"rid\": \"0\", \"file\": \"payment\", \"time\": \"2018-04-01 15:46:25\", \"type\": \"0\", \"channel\": \"14\", \"role_id\": \"63000047\", \"order_no\": \"1514048900388880384\", \"rmb_cent\": \"600\", \"role_name\": \"-1\", \"server_id\": 1063}";
		String arrst16 = "{\"lv\": \"90\", \"os\": \"android\", \"to\": \"27\", \"ts\": \"1524041778069\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"pet\", \"from\": \"25\", \"time\": \"2018-04-18 16:56:17\", \"type\": \"lv_up\", \"power\": \"37922\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"account\": \"10\", \"role_id\": \"65000879\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst17 = "{\"lv\": \"85\", \"os\": \"android\", \"ts\": \"1524041847231\", \"chn\": \"eagle_248\", \"rid\": \"1\", \"vip\": \"0\", \"file\": \"player\", \"time\": \"2018-04-18 16:57:26\", \"type\": \"dead\", \"power\": \"26382\", \"chn_id\": \"1112356\", \"is_pay\": \"0\", \"account\": \"2725\", \"role_id\": \"65000877\", \"dead_pos\": \"5696.0,3216.0\", \"dead_dgid\": \"10601\", \"role_name\": \"岳磬\", \"server_id\": 1065, \"fight_type\": \"1\", \"killer_uuid\": \"10043\"}";
		String arrst18 = "{\"ip\": \"183.236.71.86\", \"lv\": \"25\", \"os\": \"android\", \"ts\": \"1524040860293\", \"chn\": \"eagle_248\", \"mac\": \"02:00:00:00:00:00\", \"net\": \"2\", \"rid\": \"3\", \"vip\": \"0\", \"dgid\": \"1001\", \"file\": \"player_time\", \"fury\": \"0\", \"imei\": \"866119038361110\", \"imsi\": \"460077057121268\", \"time\": \"2018-04-18 16:40:59\", \"type\": \"offline\", \"login\": \"1524040832\", \"model\": \"vivoX9sPlus\", \"pos_2\": \"104010\", \"pos_3\": \"104020\", \"pos_4\": \"104030\", \"power\": \"13621\", \"chn_id\": \"1112833\", \"dg_pos\": \"6298,7938\", \"gem_lv\": \"0\", \"is_pay\": \"0\", \"player\": \"15\", \"screen\": \"1280*720\", \"summon\": \"1\", \"account\": \"2780\", \"bag_len\": \"50\", \"grahics\": \"1\", \"offline\": \"1524040860\", \"pay_sum\": \"0\", \"ps_1000\": \"60\", \"ps_1001\": \"60\", \"ps_1002\": \"0\", \"ps_1003\": \"1\", \"ps_1004\": \"0\", \"ps_1005\": \"0\", \"ps_1006\": \"0\", \"ps_1200\": \"1\", \"role_id\": \"65000897\", \"duration\": \"28\", \"guild_id\": \"0\", \"platform\": \"android\", \"used_len\": \"12\", \"gem_total\": \"0\", \"role_name\": \"束伟泽\", \"server_id\": 1065, \"birth_time\": \"1524038894\", \"remain_gold\": \"530\", \"remain_copper\": \"4808000\", \"remain_silver\": \"790\"}";
		String arrst19 = "{\"lv\": \"92\", \"os\": \"android\", \"ts\": \"1524041940444\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"quest\", \"time\": \"2018-04-18 16:59:00\", \"type\": \"main\", \"power\": \"38267\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"account\": \"10\", \"role_id\": \"65000879\", \"quest_id\": \"100750\", \"role_name\": \"越伊\", \"server_id\": 1065}";
		String arrst20 = "{\"lv\": \"80\", \"os\": \"android\", \"ts\": \"1524041294648\", \"chn\": \"eagle_248\", \"num\": \"0\", \"rid\": \"1\", \"vip\": \"0\", \"file\": \"quest_daily\", \"time\": \"2018-04-18 16:48:14\", \"type\": \"sg\", \"power\": \"24951\", \"chn_id\": \"1112356\", \"is_pay\": \"0\", \"result\": \"finish\", \"account\": \"2725\", \"is_team\": \"0\", \"role_id\": \"65000877\", \"quest_id\": \"30004\", \"role_name\": \"岳磬\", \"server_id\": 1065}";
		String arrst21 = "{\"lv\": \"67\", \"ts\": \"1524038758422\", \"rid\": \"0\", \"file\": \"rank\", \"rank\": \"16\", \"time\": \"2018-04-18 16:05:58\", \"type\": \"5000\", \"score\": \"1\", \"role_id\": \"65000889\", \"role_name\": \"令狐强炫\", \"server_id\": 1065}";
		String arrst22 = "{\"lv\": \"90\", \"os\": \"android\", \"ts\": \"1524041772778\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"file\": \"ride\", \"time\": \"2018-04-18 16:56:12\", \"type\": \"lv_up\", \"power\": \"37491\", \"chn_id\": \"274138\", \"is_pay\": \"0\", \"account\": \"10\", \"ride_lv\": \"7\", \"role_id\": \"65000879\", \"auto_buy\": \"0\", \"ride_exp\": \"35\", \"role_name\": \"越伊\", \"server_id\": 1065, \"new_ride_lv\": \"8\", \"new_ride_exp\": \"5\"}";
		String arrst23 = "{\"lv\": \"100\", \"os\": \"android\", \"ts\": \"1524040340041\", \"chn\": \"eagle_248\", \"num\": \"300\", \"rid\": \"3\", \"vip\": \"0\", \"cost\": \"30000\", \"file\": \"shop\", \"time\": \"2018-04-18 16:32:19\", \"type\": \"buy\", \"power\": \"54209\", \"chn_id\": \"1112341\", \"is_pay\": \"0\", \"account\": \"2722\", \"buy_gid\": \"1046\", \"role_id\": \"65000876\", \"role_name\": \"管若之\", \"server_id\": 1065, \"money_type\": \"11\", \"before_gold\": \"0\", \"remain_gold\": \"0\", \"before_silver\": \"5\", \"remain_copper\": \"78897\", \"remain_silver\": \"5\"}";
		String arrst24 = "{\"lv\": \"1\", \"os\": \"android\", \"ts\": \"1524040507389\", \"chn\": \"eagle_248\", \"gid\": \"11013\", \"rid\": \"1\", \"vip\": \"0\", \"file\": \"soul\", \"time\": \"2018-04-18 16:35:07\", \"type\": \"lv_up\", \"power\": \"20019\", \"chn_id\": \"1112593\", \"is_pay\": \"0\", \"new_lv\": \"2\", \"account\": \"2755\", \"hole_id\": \"1\", \"role_id\": \"65000891\", \"role_name\": \"哈绮彤\", \"server_id\": 1065}";
		String arrst25 = "{\"lv\": \"-1\", \"ts\": \"1524040149338\", \"rid\": \"0\", \"file\": \"special_mon_spawn\", \"line\": \"20\", \"time\": \"2018-04-18 16:29:09\", \"type\": \"spawn\", \"dungeon\": \"1016\", \"role_id\": \"-1\", \"actor_id\": \"11001\", \"role_name\": \"-1\", \"server_id\": 1066, \"special_mon_id\": \"202\", \"special_mon_seq_id\": \"53\"}";
		String arrst26 = "{\"hp\": \"1814225\", \"lv\": \"101\", \"os\": \"android\", \"ts\": \"1524041189429\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"dgid\": \"1009\", \"file\": \"wboss\", \"hurt\": \"1811160\", \"rank\": \"1\", \"time\": \"2018-04-18 16:46:29\", \"type\": \"kill\", \"power\": \"40526\", \"chn_id\": \"1112391\", \"is_pay\": \"0\", \"seq_id\": \"1981503872454\", \"account\": \"2730\", \"boss_id\": \"21000\", \"role_id\": \"65000882\", \"team_id\": \"1\", \"guild_id\": \"1065\", \"is_affil\": \"1\", \"role_name\": \"仲长仇血\", \"server_id\": 1065}";
		String arrst27 = "{\"lv\": \"101\", \"os\": \"android\", \"ts\": \"1524040501827\", \"chn\": \"eagle_248\", \"rid\": \"3\", \"vip\": \"0\", \"dgid\": \"1009\", \"file\": \"wboss_hurt_flag\", \"time\": \"2018-04-18 16:35:01\", \"type\": \"wboss_hurt_flag\", \"power\": \"40424\", \"chn_id\": \"1112391\", \"is_pay\": \"0\", \"seq_id\": \"1981503872454\", \"account\": \"2730\", \"boss_id\": \"21000\", \"role_id\": \"65000882\", \"hurt_date\": \"2018-04-18 11:35:01\", \"hurt_time\": \"1524022501\", \"role_name\": \"仲长仇血\", \"server_id\": 1065}";
		String arrst28 = "{\"lv\": \"-1\", \"ts\": \"1524041189418\", \"rid\": \"0\", \"dgid\": \"1009\", \"file\": \"world_boss\", \"time\": \"2018-04-18 16:46:28\", \"type\": \"world_boss\", \"killer\": \"65000882\", \"max_lv\": \"195\", \"min_lv\": \"95\", \"mon_id\": \"21000\", \"mon_lv\": \"95\", \"seq_id\": \"1981503872454\", \"role_id\": \"-1\", \"duration\": \"688\", \"hurt_num\": \"1\", \"born_time\": \"1523948998\", \"dead_time\": \"1524041189\", \"role_name\": \"-1\", \"server_id\": 1065}";

		String[] str = new String[] { arrst, arrst1, arrst2, arrst3, arrst4, arrst5, arrst6, arrst7, arrst8, arrst9,
				arrst10, arrst11, arrst12, arrst13, arrst14, arrst15, arrst16, arrst17, arrst18, arrst19, arrst20,
				arrst21, arrst22, arrst23, arrst24, arrst25, arrst26, arrst27, arrst28 };
		for (String s : str) {
			print(s);
		}
	}

	public static void print(String str) {
		JSONObject json_src = (JSONObject) JSONObject.parse(str);

		// 根节点
		JSONObject root = new JSONObject();
		root.put("who", "jyqy");
		root.put("platform", "app");

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
//			System.out.println(_k);
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
		System.out.println(root.toString());
	}
}
