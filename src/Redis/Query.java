package Redis;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

public class Query implements Runnable{
	Set<String> targets;
	JedisPool pool;
	String view;
	int type = 0; //Type: 1 - "Query single view"; 2 - "Aggregation"
	
	public void setExec(Set<String> targets,JedisPool pool){
		this.targets = targets;
		this.pool = pool;
		this.type = 2;
	}
	
	public void setExec(String view,JedisPool pool){
		this.view = view;
		this.pool = pool;
		this.type = 1;
	}
	
	public static Map<String,Double> exec(Set<String> targets,Jedis db){
		HashMap<String, Double> result = new HashMap<String,Double>();
		for(String view:targets){
			Set<Tuple> vresult = db.zrangeWithScores(view, 0, -1);
			for(Tuple t:vresult){
				result.put(t.getElement(), t.getScore());
			}
		}
		db.sync();
		Map<String,Double> rtn = sortByValues(result,Configure.TOPN);
//		UpdateList.count("Query", db);
		return rtn;
	}
	
	public static Map<String,Double> exec(String view,Jedis db){
		HashMap<String, Double> result = new HashMap<String,Double>();
		Set<Tuple> vresult = db.zrangeWithScores(view, 0, -1);
		db.sync();
		for(Tuple t:vresult){
			result.put(t.getElement(), t.getScore());
		}
		
//		UpdateList.count("Query", db);
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<K,V> sortByValues(Map<K,V> map,int TOPN){
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>(){
			@SuppressWarnings("unchecked")
			@Override
			public int compare(Map.Entry<K,V> o1, Map.Entry<K,V> o2){
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		
		Map<K,V> sortedMap = new LinkedHashMap<K,V>();
		int i = 0;
		for(Map.Entry<K, V> e : entries){
			sortedMap.put(e.getKey(), e.getValue());
			i++;
			if(i > TOPN)
				break;
		}
		
		return sortedMap;
	}

	@Override
	public void run() {
		Map<String,Double> result;
		if(type == 1){
			Jedis db = pool.getResource();
			result = Query.exec(view, db);
			pool.returnResource(db);
		}else if(type == 2){
			Jedis db = pool.getResource();
			result = Query.exec(targets, db);
			pool.returnResource(db);
		}else
			return;
		
		result.size();
	}
}

