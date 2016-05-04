package Redis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class DebugRedis {

	public static void main(String[] args) {
		Jedis db = new Jedis("localhost",6379);
		UpdateView.update("tv1", System.currentTimeMillis(), "Hello1", db);
		HashSet<String> views = new HashSet<String>();
		ArrayList<String> sviews = new ArrayList<String>();
		HashMap<String,Long> msgs = new HashMap<String,Long>();
		views.add("tv1");
		Map<String, Double> result = Query.exec(views, db);
		for(Map.Entry<String, Double> e:result.entrySet())
			System.out.println("Msg: " + e.getKey() + "\t" + e.getValue());
		System.out.println();
		views.add("tv2");
		views.add("tv3");
		result = Query.exec(views, db);
		for(Map.Entry<String, Double> e:result.entrySet())
			System.out.println("Msg: " + e.getKey() + "\t" + e.getValue());
		System.out.println();
		msgs.put("Test1",9l);
		msgs.put("Test2",10l);
		msgs.put("Test3",11l);
		msgs.put("Test4",13l);
		sviews.addAll(views);
		UpdateView.update(sviews, System.currentTimeMillis() + 5, "Duang", db);
		result = Query.exec(views, db);
		for(Map.Entry<String, Double> e:result.entrySet())
			System.out.println("Msg: " + e.getKey() + "\t" + e.getValue());
		System.out.println();
		UpdateView.update(sviews, msgs, db);
		result = Query.exec(views, db);
		for(Map.Entry<String, Double> e:result.entrySet())
			System.out.println("Msg: " + e.getKey() + "\t" + e.getValue());
		System.out.println();
		
		db.close();
		
		
		
		

	}

}
