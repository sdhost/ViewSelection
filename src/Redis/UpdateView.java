package Redis;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class UpdateView implements Runnable{
	String viewname;
	Long timestamp;
	String msg;
	JedisPool pool;
	List<String> viewnames;
	Map<String,Long> messages;
	int type = 0;//Type:  1 "One to One"
			 	//		  2 "One to Multi"
			 	//	 	  3 "Multi to Multi"
	
	public void setUpdate(String viewname, Long timestamp, String msg, JedisPool pool){
		this.viewname = viewname;
		this.timestamp = timestamp;
		this.msg = msg;
		this.pool = pool;
		this.type = 1;
	}
	
	public void setUpdate(List<String> viewnames, Long timestamp, String msg, JedisPool pool){
		this.viewnames = viewnames;
		this.timestamp = timestamp;
		this.msg = msg;
		this.pool = pool;
		this.type = 2;
	}
	
	public void setUpdate(List<String> viewnames, Map<String,Long> messages, JedisPool pool){
		this.viewnames = viewnames;
		this.messages = messages;
		this.pool = pool;
		this.type = 3;
	}
	
	public static void update(String viewname, Long timestamp, String msg, Jedis db){
		//Add a new message to view
		Long size = db.zcard(viewname);
		if(size > Configure.TOPN){
			Pipeline pp = db.pipelined();
			pp.multi();
			pp.zadd(viewname, timestamp, msg);
			pp.zremrangeByRank(viewname, 0, size - Configure.TOPN - 1);
			pp.exec();
			pp.syncAndReturnAll();
		}else{
			db.zadd(viewname, timestamp, msg);
			db.sync();
		}
//		UpdateList.count("View", db);
		
	}
	
	public static void update(List<String> viewnames, Long timestamp, String msg, Jedis db){
		//Add a new message to multiple views
		Pipeline pp = db.pipelined();
		pp.multi();
		for(String name:viewnames){
			pp.zadd(name, timestamp, msg);
		}
		pp.exec();
		pp.syncAndReturnAll();
		for(String name:viewnames){
			Long size = db.zcard(name);
			if(size > Configure.TOPN){
				db.zremrangeByRank(name, 0, size - Configure.TOPN - 1);
			}
		}
		db.sync();
//		UpdateList.count("View", db);
	}
	
	public static void update(List<String> viewnames, Map<String,Long> messages, Jedis db){
		//Add multiple messages to multiple views
		Pipeline pp = db.pipelined();
		pp.multi();
		for(Map.Entry<String, Long> e : messages.entrySet())
			for(String name:viewnames){
				pp.zadd(name, e.getValue(), e.getKey());
			}
		pp.exec();
		pp.syncAndReturnAll();
		for(String name:viewnames){
			Long size = db.zcard(name);
			if(size > Configure.TOPN){
				db.zremrangeByRank(name, 0, size - Configure.TOPN - 1);
			}
		}
		db.sync();
//		UpdateList.count("View", db);
	}

	@Override
	public void run() {
		if(type == 0)
			return;
		else if(type == 1){
			Jedis db = pool.getResource();
			UpdateView.update(viewname, timestamp, msg, db);
			pool.returnResource(db);
		}else if(type == 2){
			Jedis db = pool.getResource();
			UpdateView.update(viewnames, timestamp, msg, db);
			pool.returnResource(db);
		}else if(type == 3){
			Jedis db = pool.getResource();
			UpdateView.update(viewnames, messages, db);
			pool.returnResource(db);
		}else
			return;
	}
}
