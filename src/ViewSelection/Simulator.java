package ViewSelection;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import JMSRole.JMSClientQueueProducer;
import Runtime.TopologyGenerator;
import Runtime.Dataset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;

import jline.internal.Log;

/**
 * Created by kaijichen on 11/6/14.
 */
public class Simulator {
//    public static String mqURL = "tcp://54.155.127.232:61616";
    public static String mqURL = "tcp://localhost:61616";
    private static String name;
    public static Integer minStep = 100;
    private static long duration;
    private static long interval;
    public static int feedcount = 6;
    public static int spoutEmit = 500000000;
    public static PrintStream ps;
    private static boolean isdebug = false;
    public static double costratio = 3;
    public static int type = 2;
    private static int max_pending = 500;
    //private static final Logger LOG = Logger.getLogger(Simulator.class);
    private static int FEED_COUNT = 1000;
    private static int USER_COUNT = 3000;
    private static int MEAN_FEED_INTV = 1500;
    private static int MEAN_USER_FOLLOW = 5;
    private static int MEAN_USER_INTV = 100;
    public static int ZIPF_SAMPLE_SIZE = 200;
    public static void main(String[] args) throws Exception {
    	
    	//Logger.getRootLogger().setLevel(Level.WARN);
//    	File f = new File("Log"+System.currentTimeMillis());
//    	f.createNewFile();
//    	FileOutputStream fos = new FileOutputStream(f);
//    	ps = new PrintStream(fos);
//    	Log.setOutput(ps);
//    	String filename = "Feed"+String.valueOf(FEED_COUNT)+"User"+String.valueOf(USER_COUNT)+".FZ99";
    	
//    	ZipfDistribution rnd = new ZipfDistribution(10000,0.48);
//    	System.out.println(rnd.sample());
    	
    	String fname = "gplus_combined.txt.compress.ds";
//    	Dataset ds = new Dataset();
//    	ds.loadEdges(fname);
//    	ds.storeStat(fname);
//    	ds.storeDataset(fname);
    	assignTopology(fname);
    	fname = "twitter_combined.txt.ds";
    	assignTopology(fname);
    	fname = "facebook_combined.txt.ds";
    	assignTopology(fname);
    	
//    	generateTopology(filename);
//    	String filename = "ZipfianP0.32S500M200.txt";
//    	genZipfSamples(0.32,500,200,filename);
    	
//    	Dataset ds = new Dataset();
//    	ds.loadEdges(fname);
//    	ds.storeDataset(fname+".ds");

//        generateTopology(filename);
//        TopologyGenerator topology = getTopology(filename);
//        simulateGreedy(topology);
        
//        int type = 99;
//        if(args.length == 4){
//            name = args[0];
//            mqURL = "tcp://" + args[1] + ":61616";
//            duration = Long.valueOf(args[2]);
//            type = Integer.valueOf(args[3]);
//            System.out.println("Name\t" + name);
//            System.out.println("ActiveMQ URL\t" + mqURL );
//            System.out.println("Simulation Duration\t" + duration / 60000 + " min(s)");
//            System.out.println("Type = " + type);
//            System.out.println("FeedCount = " + feedcount);
//        }else if(args.length == 0){
//            System.out.println("Use default value");
//            System.out.println("Name\t" + name);
//            System.out.println("ActiveMQ URL\t" + mqURL );
//            System.out.println("Simulation Duration\t" + duration / 60000 + " min(s)");
//            System.out.println("Type = " + type);
//            System.out.println("FeedCount = " + feedcount);
//            
//        }else if(args.length == 1){
//        	type = Integer.valueOf(args[0]);
//        	System.out.println("Name\t" + name);
//            System.out.println("ActiveMQ URL\t" + mqURL );
//            System.out.println("Simulation Duration\t" + duration / 60000 + " min(s)");
//            System.out.println("Type = " + type);
//            System.out.println("FeedCount = " + feedcount);
//        }else if(args.length == 2){
//        	//mqURL = "tcp://" + args[0] + ":61616";
//        	type = Integer.valueOf(args[0]);
//        	feedcount = Integer.valueOf(args[1]);
//        	System.out.println("Name\t" + name);
//            System.out.println("ActiveMQ URL\t" + mqURL );
//            System.out.println("Simulation Duration\t" + duration / 60000 + " min(s)");
//            System.out.println("Type = " + type);
//            System.out.println("FeedCount = " + feedcount);
//        }else if(args.length == 3){
//        	mqURL = "tcp://" + args[0] + ":61616";
//        	type = Integer.valueOf(args[1]);
//        	feedcount = Integer.valueOf(args[2]);
//        	System.out.println("Name\t" + name);
//            System.out.println("ActiveMQ URL\t" + mqURL );
//            System.out.println("Simulation Duration\t" + duration / 60000 + " min(s)");
//            System.out.println("Type = " + type);
//            System.out.println("FeedCount = " + feedcount);
//        }else{
//            System.out.println("Wrong arguments provided,\n TopologyName ActiveMQ_Node_IP SimulationDuration(ms)");
//            return;
//        }
//        try{
//            if(type == 0)
//                simulatePush();
//            else if (type == 1)
//                simulatePull();
//            else if (type == 2)
//                simulateSharedPush();
//            else if (type == 3)
//            	simulatePushThroughput();
//            else if (type == 4)
//            	simulatePullThroughput();
//            else if (type == 5)
//            	simulateSharedPushThroughput();
//            else{
//                System.out.println("Unsupported Type " + type);
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }
    }
    
    private static void genZipfSamples(double parameter,int count,int mean,String filename) throws IOException{
    	ZipfDistribution rnd = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,parameter);
    	int[] samples = new int[count];
    	for(int i=0;i<count;i++){
    		samples[i] = rnd.sample();
    	}
    	int max = 0;
    	File out = new File(filename);
		out.createNewFile();
    	FileOutputStream outs = new FileOutputStream(out);
    	PrintStream outps = new PrintStream(outs);
    	for(int i=0;i<samples.length;i++)
    		if(samples[i] > max)
    			max = samples[i];
    	double num_mean = rnd.getNumericalMean();
    	
    	outps.println("Zipf: " + parameter + "\t Count: " + count + "\t Mean: " + mean);
    	outps.println("Num_Mean: " + num_mean + "\t Max: " + max);
    	for(int i=0;i<samples.length;i++)
    		outps.println((int) Math.ceil((max - (double)samples[i])/max*(double)mean) + 2);
    	
    	outps.close();
    	
    }
    
    private static void simulateGreedy(TopologyGenerator topology){
    	TopologyBuilder build = null;
    	if(type == 2)
    	 build = topology.topologyBuilderGreedy();
    	else if(type == 1)
    		build = topology.topologyBuilderPushThroughput();
    	else if(type == 0)
    		build = topology.topologyBuilderPullThroughput();
    	else if(type == 3)
    		build = topology.topologyBuilderHierachyGreedy();
    	else if(type == 10)
    		build = topology.topologyBuilderPullThroughputPassive();
    	else if(type == 11)
    		build = topology.topologyBuilderPullThroughputPassive();
    	else if(type == 12)
    		build = topology.topologyBuilderGreedyPassive();
    	else if(type == 13)
    		build = topology.topologyBuilderHierachyGreedyPassive();
    	else
    		return;
		Config config = new Config();
	    config.setDebug(false);
	    config.setMaxSpoutPending(max_pending);
	    config.setNumWorkers(16);
//	    LocalCluster cluster = new LocalCluster();
//	    cluster.submitTopology(name, config,build.createTopology());
     
	    
	    try {
	    	StormSubmitter.submitTopology(name, config,build.createTopology());
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
    }
    
    private static TopologyGenerator getTopology(String filename){
    	try {
			return new TopologyGenerator(filename);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    }
    
    private static void generateTopology(String filename) throws IOException{
    	int i = 0;
    	TopologyGenerator topology = null;
    	TopologyGenerator lastStored = null;
    	int c = 0;
    	int n = 0;
//    	while(i<1900){
    		topology = null;
    		HashSet<FeedInfo> feeds = FeedGenerator.genZipfFeed(FEED_COUNT, mqURL, MEAN_FEED_INTV, 0.57);
//	    	HashSet<UserInfo> users = UserGenerator.genZipfUser(USER_COUNT, MEAN_USER_FOLLOW, MEAN_USER_INTV, feeds, 0.62,0.62);
    		HashSet<UserInfo> users = UserGenerator.genZipfUser_feed(USER_COUNT, MEAN_USER_FOLLOW, MEAN_USER_INTV, feeds, 0.62,0.62,0.99);
	    	System.out.println("Generated");
	    	
//	    	HashSet<FeedInfo> feeds = FeedGenerator.genFeed(20,mqURL,2,50);
//	    	HashSet<UserInfo> users = UserGenerator.genUniformUser(40, 6, feeds, 2,40 );
//	    	HashSet<FeedInfo> feeds = FeedGenerator.genConstantFeed(20,mqURL,2);
//	    	HashSet<UserInfo> users = UserGenerator.genConstantUser(40, 5, feeds, 40);
	    	topology = new TopologyGenerator(users,feeds);
	    	System.out.println("Object created");
	    	topology.storeDataset(filename);
	    	System.out.println("Stored");
	    	long begin = System.currentTimeMillis();
//	    	topology.topologyBuilderGreedy();
	    	topology.greedyViews();
//	    	topology.hierachyGreedyViews();
	    	System.out.println((System.currentTimeMillis() - begin) / 1000 + " seconds");
	    	i = topology.debugMatViewCount;
	    	if(i>0)
	    		n++;
	    	if(lastStored == null || i > lastStored.debugShareViews)
	    		lastStored = topology;
	    	System.out.println(c + " Runs finished, " + i + " materialized views");
	    	c++;
//    	}
    	System.out.println("TotalView:" + topology.debugAllViewCount + "\tMatView:" + topology.debugMatViewCount);
    	System.out.println("SharedView:" + topology.debugShareViews + "\tAverageFollow:" + (double)topology.debugAverageFollow / USER_COUNT);
//    	topology.storeDataset(filename);
    	System.out.println("Finished");

    }
    
    private static void assignTopology(String filename) throws IOException{
    	int i = 0;
    	TopologyGenerator topology = null;
    	TopologyGenerator lastStored = null;
    	int c = 0;
    	int n = 0;
    	long begin = System.currentTimeMillis();
    	Dataset ds = new Dataset(filename);
    	System.out.println("LoadData: " + (System.currentTimeMillis() - begin) + " ms");
//    	while(i<1900){
    		topology = null;
    		HashSet<FeedInfo> feeds = FeedGenerator.assignZipfFeed(ds.feeds, MEAN_FEED_INTV, 0.57);
	    	HashSet<UserInfo> users = UserGenerator.assignZipfUser(ds.users, ds.feeds, MEAN_USER_INTV, 0.62);
	    	System.out.println("Generated");
//	    	HashSet<FeedInfo> feeds = FeedGenerator.genFeed(20,mqURL,2,50);
//	    	HashSet<UserInfo> users = UserGenerator.genUniformUser(40, 6, feeds, 2,40 );
//	    	HashSet<FeedInfo> feeds = FeedGenerator.genConstantFeed(20,mqURL,2);
//	    	HashSet<UserInfo> users = UserGenerator.genConstantUser(40, 5, feeds, 40);
	    	topology = new TopologyGenerator(users,feeds);
	    	System.out.println("Object created");

	    	topology.storeDataset(filename+".t");
	    	System.out.println("Stored");
//	    	begin = System.currentTimeMillis();
////	    	topology.topologyBuilderGreedy();
//	    	topology.greedyViews();
////	    	topology.hierachyGreedyViews();
//	    	System.out.println("Plan Generating: " + (System.currentTimeMillis() - begin) + " ms");
//	    	i = topology.debugMatViewCount;
//	    	if(i>0)
//	    		n++;
//	    	if(lastStored == null || i > lastStored.debugShareViews)
//	    		lastStored = topology;
//	    	System.out.println(c + " Runs finished, " + i + " materialized views");
//	    	c++;
////    	}
//    	System.out.println("TotalView:" + topology.debugAllViewCount + "\tMatView:" + topology.debugMatViewCount);
//    	System.out.println("SharedView:" + topology.debugShareViews + "\tAverageFollow:" + (double)topology.debugAverageFollow / USER_COUNT);
//    	System.out.println("Finished");

    }

//    private static void simulatePush() throws Exception{
//        HashSet<FeedInfo> feeds = FeedGenerator.genFeed(feedcount,mqURL);
//        //HashSet<UserInfo> users = UserGenerator.genUniformUser(20,5,feeds);
//        HashSet<UserInfo> users = UserGenerator.genHierachyUser(feeds,mqURL);
//        TopologyGenerator topology = new TopologyGenerator(users,feeds);
//        topology.init(true);
//        TopologyBuilder build = topology.topologyBuilderPush();
//        Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(10);
//        config.setNumWorkers(16);
//        StormSubmitter.submitTopology(name, config,build.createTopology());
////        LocalCluster cluster = new LocalCluster();
////        cluster.submitTopology(name, config,build.createTopology());
//
//        long begin = System.currentTimeMillis();
//        Random rnd = new Random(begin);
//        while(System.currentTimeMillis() - begin < duration){
//            Thread.sleep(interval);
//            for(Map.Entry<Integer,JMSClientQueueProducer> entry:topology.getFeedProducer().entrySet()){
//                if(rnd.nextInt(10) > 5){
//                    entry.getValue().setMessage("Message from" + entry.getKey() + " @ " + System.currentTimeMillis());
//                    entry.getValue().send();
//                }
//            }
//        }
//    }

//    private static void simulatePull() throws Exception{
//        HashSet<FeedInfo> feeds = FeedGenerator.genFeed(feedcount,mqURL,2,5);
//        HashSet<UserInfo> users = UserGenerator.genUniformUser(6,2,feeds,mqURL,1.5,10.5);
////        HashSet<UserInfo> users = UserGenerator.genHierachyUser(feeds,mqURL);
//        TopologyGenerator topology = new TopologyGenerator(users,feeds);
//        topology.init(true);
//        TopologyBuilder build = topology.topologyBuilderPull();
//        users = topology.getUsers();
//        Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(10);
//        config.setNumWorkers(16);
////        StormSubmitter.submitTopology(name, config,build.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(name, config,build.createTopology());
//
////        long begin = System.currentTimeMillis();
////        Random rnd = new Random(begin);
////        long last = begin;
////        while(System.currentTimeMillis() - begin < duration){
////            Thread.sleep(interval);
////            for(Map.Entry<Integer,JMSClientQueueProducer> entry:topology.getFeedProducer().entrySet()){
////                if(rnd.nextInt(10) > 5){
////                    entry.getValue().setMessage("Message from" + entry.getKey() + " @ " + System.currentTimeMillis());
////                    entry.getValue().send();
////                }
////            }
////            if(System.currentTimeMillis() - last > 5 * interval){
////                last = System.currentTimeMillis();
////                for(UserInfo ui : users){
////                    ui.getNotifier().setMessage("Pull");
////                    ui.getNotifier().send();
////                }
////            }
////        }
//    }

//    private static void simulateSharedPush() throws Exception{
//        HashSet<FeedInfo> feeds = FeedGenerator.genFeed(feedcount,mqURL);
//        //HashSet<UserInfo> users = UserGenerator.genUniformUser(20,5,feeds);
//        HashSet<UserInfo> users = UserGenerator.genHierachyUser(feeds,mqURL);
//        TopologyGenerator topology = new TopologyGenerator(users,feeds);
//        topology.init(true);
//        TopologyBuilder build = topology.topologyBuilderSharedPush();
//        Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(10);
//        config.setNumWorkers(16);
//        StormSubmitter.submitTopology(name, config,build.createTopology());
////        LocalCluster cluster = new LocalCluster();
////        cluster.submitTopology(name, config,build.createTopology());
//
//        long begin = System.currentTimeMillis();
//        Random rnd = new Random(begin);
//        while(System.currentTimeMillis() - begin < duration){
//            Thread.sleep(interval);
//            for(Map.Entry<Integer,JMSClientQueueProducer> entry:topology.getFeedProducer().entrySet()){
//                if(rnd.nextInt(10) > 5){
//                    entry.getValue().setMessage("Message from" + entry.getKey() + " @ " + System.currentTimeMillis());
//                    entry.getValue().send();
//                }
//            }
//        }
//    }
    
//    private static void simulatePushThroughput() throws Exception{
//        HashSet<FeedInfo> feeds = FeedGenerator.genFeed(feedcount,mqURL);
//        HashSet<UserInfo> users = UserGenerator.genHierachyUser(feeds,mqURL);
//        TopologyGenerator topology = new TopologyGenerator(users,feeds);
//        topology.init(true);
//        TopologyBuilder build = topology.topologyBuilderPushThroughput();
//        Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(1);
//        config.setNumWorkers(1);
//        StormSubmitter.submitTopology(name, config,build.createTopology());
////        LocalCluster cluster = new LocalCluster();
////        cluster.submitTopology(name, config,build.createTopology());
//
//        Thread.sleep(duration);
////        cluster.killTopology(name);
//    }

//    private static void simulatePullThroughput() throws Exception{
//        HashSet<FeedInfo> feeds = FeedGenerator.genFeed(feedcount,mqURL);
//        //HashSet<UserInfo> users = UserGenerator.genUniformUser(20,5,feeds,mqURL);
//        HashSet<UserInfo> users = UserGenerator.genHierachyUser(feeds,mqURL);
//        TopologyGenerator topology = new TopologyGenerator(users,feeds);
//        topology.init(true);
//        TopologyBuilder build = topology.topologyBuilderPullThroughput();
//        users = topology.getUsers();
//        Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(3);
//        config.setNumWorkers(5);
////        StormSubmitter.submitTopology(name, config,build.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(name, config,build.createTopology());
//
//        long begin = System.currentTimeMillis();
//        while(System.currentTimeMillis() - begin < duration){
//            Thread.sleep(interval * 5);
//	        for(UserInfo ui : users){
//	            ui.getNotifier().setMessage("Pull");
//	            ui.getNotifier().send();
//	        }
//        }
////        cluster.killTopology(name);
//    }

//    private static void simulateSharedPushThroughput() throws Exception{
//        HashSet<FeedInfo> feeds = FeedGenerator.genFeed(feedcount,mqURL);
//        //HashSet<UserInfo> users = UserGenerator.genUniformUser(20,5,feeds);
//        HashSet<UserInfo> users = UserGenerator.genHierachyUser(feeds,mqURL);
//        TopologyGenerator topology = new TopologyGenerator(users,feeds);
//        topology.init(true);
//        TopologyBuilder build = topology.topologyBuilderSharedPushThroughput();
//        Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(3);
//        config.setNumWorkers(5);
//        StormSubmitter.submitTopology(name, config,build.createTopology());
////        LocalCluster cluster = new LocalCluster();
////        cluster.submitTopology(name, config,build.createTopology());
//
//        Thread.sleep(duration);
////        cluster.killTopology(name);
//    }
    
//    private static void debug() throws Exception{
//    	HashSet<FeedInfo> feeds = FeedGenerator.genFeed(5,mqURL,0.5,3);
//    	HashSet<UserInfo> users = UserGenerator.genUniformUser(7, 2, feeds, mqURL, 0.5, 5);
//    	TopologyGenerator topology = new TopologyGenerator(users,feeds);
////    	topology.init(true);
//    	TopologyBuilder build = topology.topologyBuilderGreedy();
////    	TopologyBuilder build = topology.topologyBuilderPullThroughput();
//    	Config config = new Config();
//        config.setDebug(isdebug);
//        config.setNumAckers(3);
//        config.setNumWorkers(5);
//        StormTopology tp = build.createTopology();
//        for(Map.Entry<String, Bolt> e:tp.get_bolts().entrySet()){
//        	System.out.println(e.getKey() + "\t" + e.getValue().get_common().get_inputs_size());
//        }
//        
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(name, config,build.createTopology());
//        
//        Thread.sleep(duration);
//    }
    
    
}
