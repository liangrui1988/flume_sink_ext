package flume_mysql;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;

import com.flume.dome.mysink.DBsqlSink;

public class TestMain {

	public TestMain() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) throws EventDeliveryException {
		DBsqlSink sink=new DBsqlSink();
		Context c=new Context();
		c.put("type", "com.flume.dome.mysink.DBsqlSink");
		c.put("hostname", "jdbc:postgresql://192.168.20.243:5432");
		c.put("port", "5432");
		c.put("databaseName", "game_log");
		c.put("user", "game");
		c.put("password", "game123");
		c.put("serverId", "1");
		c.put("tableName", "zl_log_info");

		sink.configure(c);
		sink.start();
		
		sink.process();
		
		
	}

}
