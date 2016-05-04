package ViewSelection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import Runtime.Dataset;
import Runtime.RedisTopology;

public class SimulatorRedis {

	private static int count = 50000000;
	private static int type = 2;
	
	public static String url = "localhost";
	public static int port = 6379;
	
	public static int PARALLEL_View = 10;
	public static int PARALLEL_Query = 10;
	
	public static void main(String[] args) {
		String filename = "Feed20User40_zipf.txt";
		
		int server_type = 0;
		if(args.length == 2){
        	type = Integer.valueOf(args[0]);
        	server_type = Integer.valueOf(args[1]);
        }else if(args.length == 3){
        	type = Integer.valueOf(args[0]);
        	server_type = Integer.valueOf(args[1]);
        	filename = args[2];
        }
		
		Dataset ds = null;
		try {
			ds = new Dataset(filename);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		Algorithm.init(ds.users, ds.feeds);
		HashMap<View,ArrayList<View>> selection = null;
		if(type == 2)
	    	 selection = Algorithm.greedyViews();
    	else if(type == 1)
    		selection = Algorithm.simpleViews(true);
    	else if(type == 0)
    		selection = Algorithm.simpleViews(false);
    	else if(type == 3)
    		selection = Algorithm.hierachyGreedyViews();
    	else
    		return;
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(100);
		config.setBlockWhenExhausted(true);

		JedisPool pool1 = new JedisPool(config,url,port);
//		JedisPool pool2 = new JedisPool(config,url,port);
		RedisTopology sim1 = new RedisTopology(Algorithm.users,Algorithm.feeds,selection,pool1);
//		RedisTopology sim2 = new RedisTopology(Algorithm.users,Algorithm.feeds,selection,pool2);
//		sim.combineSim(count);
		
		if(server_type == 1){
			sim1.setExec(1, count, true);
			Thread maintainer = new Thread(sim1);
			maintainer.start();
		}else{
			sim1.setExec(2, count, true);
			Thread processor = new Thread(sim1);
			processor.start();
		}
		

	}

}
