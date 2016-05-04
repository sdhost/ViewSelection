package Redis;

import redis.clients.jedis.Jedis;

public class UpdateList {
	public static void update(String listname,String msg, Jedis db){
		db.rpush(listname, msg);
	}
	
	public static void count(String view, Jedis db){
		db.rpush(view, "");
		db.sync();
	}
}
