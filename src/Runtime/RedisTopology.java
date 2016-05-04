package Runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import Redis.Query;
import Redis.UpdateView;
import ViewSelection.FeedInfo;
import ViewSelection.SimulatorRedis;
import ViewSelection.UserInfo;
import ViewSelection.View;


public class RedisTopology implements Runnable{
	
	 private HashSet<FeedInfo> feeds;
	 private HashSet<UserInfo> users;
	 
	 private HashMap<View,ArrayList<View>> plan;
	 private HashMap<Integer,ArrayList<String>> updateGraph;
	 private HashMap<Integer,ArrayList<Integer>> timeSchedulesFeed;
	 private HashMap<Integer, ArrayList<UserInfo>> timeSchedulesPull;
//	 private Integer maxUpdateFreq = 0;
	 
	 
	 private JedisPool pool;
	 private Jedis logdb;
	 private int exec_type = 0;//Exec type, 1 for view maintainer, 2 for query processor, 0 for both.
	 private int exec_count = 10000;
	 private boolean multi = false;
	 private ExecutorService exe_pool;
	 
	 private long emitted = 0;
	 private long transferred = 0;
	 private long lemitted = 0;
	 private long ltime = 0;
	 
	 private long interval = 1000;
	 
	 public String outputView;
	
	 
	 public RedisTopology(HashSet<UserInfo> users, HashSet<FeedInfo> feeds, HashMap<View,ArrayList<View>> plan, JedisPool pool){
		 this.feeds = feeds;
		 this.users = users;
		 this.setPlan(plan);
		 this.pool = pool;
		 logdb = new Jedis(SimulatorRedis.url,SimulatorRedis.port);
	 }
	 
	 public void viewMaintainer(int count){
//		 emitted = 0;
//		 transferred = 0;
		 ltime = System.currentTimeMillis(); 
		 for(long i = 1; i < count; i++){
			 for(Integer key:this.timeSchedulesFeed.keySet()){
				 if(i % key == 0){
					 try(Jedis db = this.pool.getResource()){
						 for(Integer f:this.timeSchedulesFeed.get(key)){
							 UpdateView.update(this.updateGraph.get(f), i,"F" + f + ":" + i,db);
//							 transferred += this.updateGraph.get(f).size();
						 }
						 this.pool.returnResource(db);
					 } 
					 emitted += this.timeSchedulesFeed.get(key).size();
					 if(System.currentTimeMillis() - ltime > interval)
						 this.countThroughput();
				 }
			 }
		 }
	 }
	 
	 public void viewMaintainerMulti(int count){
//		 emitted = 0;
//		 transferred = 0;
		 ltime = System.currentTimeMillis();
		 for(long i = 1; i < count; i++){
			 for(Integer key:this.timeSchedulesFeed.keySet()){
				 if(i % key == 0){
					 for(Integer f:this.timeSchedulesFeed.get(key)){
						 UpdateView upd = new UpdateView();
						
						 upd.setUpdate(this.updateGraph.get(f), i,"F" + f + ":" + i,pool);
						 exe_pool.execute(upd);
//						 transferred += this.updateGraph.get(f).size();
					 }
					 emitted += this.timeSchedulesFeed.get(key).size();
					 if(System.currentTimeMillis() - ltime > interval)
						 this.countThroughput();
				 }
			 }
		 }
	 }
	 
	 public void queryProcessor(int count){
		 for(long i = 1; i < count; i++){
			 for(Integer key:this.timeSchedulesPull.keySet()){
				 if(i % key == 0){
					 try(Jedis db = this.pool.getResource()){
						 for(UserInfo ui:this.timeSchedulesPull.get(key)){
							 Map<String,Double> news = Query.exec(ui.getQuery(), db);
							 news.size();
						 }
						 this.pool.returnResource(db);
					 } 
				 }
			 }
		 }
	 }
	 public void queryProcessorMulti(int count){
		 for(long i = 1; i < count; i++){
			 for(Integer key:this.timeSchedulesPull.keySet()){
				 if(i % key == 0){
						 for(UserInfo ui:this.timeSchedulesPull.get(key)){
							 Query qy = new Query();
							 qy.setExec(ui.getQuery(), pool);
							 exe_pool.execute(qy);
						 }
				 }
			 }
		 }
	 }
	 
	 public void combineSim(int count){
//		 emitted = 0;
//		 transferred = 0;
		 ltime = System.currentTimeMillis();
		 for(long i = 1; i < count; i++){
			 for(Integer key:this.timeSchedulesFeed.keySet()){
				 if(i % key == 0){
					 Jedis db = null;
					 try{
						 db = this.pool.getResource();
						 for(Integer f:this.timeSchedulesFeed.get(key)){
							 UpdateView.update(this.updateGraph.get(f), i,"F" + f + ":" + i,db);
//							 transferred += this.updateGraph.get(f).size();
						 }
					 }finally{
						 if(db != null)
							this.pool.returnBrokenResource(db);
					 }
					 emitted += this.timeSchedulesFeed.get(key).size();
					 if(System.currentTimeMillis() - ltime > interval)
						 this.countThroughput();
				 }
			 }
			 for(Integer key:this.timeSchedulesPull.keySet()){
				 if(i % key == 0){
					 Jedis db = null;
					 try{
						 db = this.pool.getResource();
						 for(UserInfo ui:this.timeSchedulesPull.get(key)){
							 Map<String,Double> news = Query.exec(ui.getQuery(), db);
							 news.size();
						 }
					 }finally{
						 if(db != null)
							 this.pool.returnBrokenResource(db);
					 }
				 }
			 }
		 }
	 }
	 
	 private void countThroughput(){
		 long current = System.currentTimeMillis();
//		 long emitted = logdb.llen("View");
		 if(ltime != 0){
			 String msg = emitted + " " + (double)(emitted - lemitted) / (double)(current - ltime) * 1000 + " " + current;
			 System.out.println(msg);
			 Redis.UpdateList.update("Metric", msg, logdb);
		 }
		 ltime = current;
		 lemitted = emitted;
	 }
	 
	 private void setPlan(HashMap<View,ArrayList<View>> plan){
		 this.plan = plan;
		 initTopology(); 
	 }
	
	 private void initTopology(){
		 if(this.feeds == null || this.feeds.isEmpty() || this.users == null || this.users.isEmpty()){
			 System.out.println("Feeds and users not set");
			 return;
		 }
		 this.updateGraph = new HashMap<Integer,ArrayList<String>>();
		 this.timeSchedulesFeed = new HashMap<Integer,ArrayList<Integer>>();
		 this.timeSchedulesPull = new HashMap<Integer,ArrayList<UserInfo>>();
		 for(View v:plan.keySet()){
			 if(v.isFeed){
				 if(v.updateIntv == 0)
					 System.out.println("Error! View " + v.id + " has a 0 update frequency");
//				 if(this.maxUpdateFreq < v.updateFreq)
//					 this.maxUpdateFreq = (int)Math.round(v.updateFreq);
				 if(!this.timeSchedulesFeed.containsKey(v.updateIntv)){
					 ArrayList<Integer> toFire = new ArrayList<Integer>();
					 toFire.add(Integer.valueOf(v.id.substring(1)));
					 this.timeSchedulesFeed.put(v.updateIntv, toFire);
				 }else{
					 this.timeSchedulesFeed.get(v.updateIntv).add(Integer.valueOf(v.id.substring(1)));
				 }
						 
				 if(!this.updateGraph.containsKey(Integer.valueOf(v.id.substring(1)))){
					 ArrayList<String> toUpdate = new ArrayList<String>();
					 toUpdate.add(v.id);
					 this.updateGraph.put(Integer.valueOf(v.id.substring(1)), toUpdate);
				 }
			 }else{
				 if(v.isMaterialized){
					 for(Integer f:v.feeds){
						 if(!this.updateGraph.containsKey(f)){
							 ArrayList<String> toUpdate = new ArrayList<String>();
							 toUpdate.add(v.id);
							 this.updateGraph.put(Integer.valueOf(v.id.substring(1)), toUpdate);
						 }else{
							 this.updateGraph.get(f).add(v.id);
						 }
					 }
						 
				 }
			 }
		 }
		 for(UserInfo ui:this.users){
			 if(!ui.isPush()){
				 if(ui.getRequestIntv() == 0)
					 System.out.println("Error! User " + ui.getId() + " has a 0 update frequency");
				 
				 if(!this.timeSchedulesPull.containsKey(ui.getRequestIntv())){
					 ArrayList<UserInfo> toPull = new ArrayList<UserInfo>();
					 toPull.add(ui);
					 this.timeSchedulesPull.put(ui.getRequestIntv(), toPull);
				 }else{
					 this.timeSchedulesPull.get(ui.getRequestIntv()).add(ui);
				 }
			 }
		 }
		 System.out.println("Initialized");
		 System.out.println("For Feeds:");
		 for(int key:this.timeSchedulesFeed.keySet())
			 System.out.println(key + " : " + this.timeSchedulesFeed.get(key).size());
		 System.out.println("For Users");
		 for(int key:this.timeSchedulesPull.keySet())
			 System.out.println(key + " : " + this.timeSchedulesPull.get(key).size());
	 }

	public void setExec(int type, int count, boolean multi){
		this.exec_type = type;
		this.exec_count = count;
		this.multi = multi;
			
	}
	 
	@Override
	public void run() {
		
		if(exec_type == 0)
			this.combineSim(this.exec_count);
		else if(exec_type == 1)
			if(multi){
				this.exe_pool = Executors.newFixedThreadPool(SimulatorRedis.PARALLEL_View);
				this.viewMaintainerMulti(exec_count);
				this.exe_pool.shutdown();
			}else
				this.viewMaintainer(exec_count);
		else if(exec_type == 2)
			if(multi){
				this.exe_pool = Executors.newFixedThreadPool(SimulatorRedis.PARALLEL_Query);
				this.queryProcessorMulti(exec_count);
				this.exe_pool.shutdown();
			}else
				this.queryProcessor(exec_count);
		else
			System.err.println("Unknown type: " + exec_type);
			
	}
	 
}
