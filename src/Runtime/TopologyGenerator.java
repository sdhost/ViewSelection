package Runtime;

import ViewSelection.Algorithm;
import ViewSelection.FeedInfo;
import ViewSelection.Simulator;
import ViewSelection.UserInfo;
import ViewSelection.View;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import bolt.JmsBufferedBolt;
import bolt.JmsMultiBufferedBolt;
import bolt.LatencyCollectBolt;
import bolt.MultiSortByTimeBolt;
import bolt.NotifierBolt;
import bolt.SortByTimeBolt;
import bolt.ThroughputCountBolt;
import bolt.ThroughputCountMultiBolt;
import bolt.VirtualJmsBolt;
import spout.NotifierSpout;
import spout.TimerSpout;
import spout.VirtualJmsSpout;
import JMSRole.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.*;

import jline.internal.Log;

/**
 * Created by kaijichen on 10/27/14.
 */

public class TopologyGenerator {
//    private final int TOPN = 10;
    private HashSet<FeedInfo> feeds;
    private HashSet<UserInfo> users;

    private HashMap<Integer,JMSClientQueueProducer> feedProducer;
    private HashMap<Integer,JMSClientTopicConsumer> userConsumer;


    //For Debug
    public int debugMatViewCount = 0;
    public int debugAllViewCount = 0;
    public int debugAverageFollow = 0;
    public int debugShareViews = 0;
    
//    private int linkCount = 0;


    public TopologyGenerator(HashSet<UserInfo> users, HashSet<FeedInfo> feeds){
        this.users = users;
        this.feeds = feeds;
    }

    public void init(boolean onlyLat){
        if(feeds == null || users == null)
            return;
        this.feedProducer = new HashMap<Integer, JMSClientQueueProducer>();
        for(FeedInfo fi:feeds){
            JMSClientQueueProducer producer = new JMSClientQueueProducer(fi.getURL(),fi.getQueueName(),String.valueOf(fi.getId()));
            this.feedProducer.put(fi.getId(),producer);
        }
        if(!onlyLat) {
            this.userConsumer = new HashMap<Integer, JMSClientTopicConsumer>();
            for (UserInfo ui : users) {
                for (Integer fid : ui.getFollow())
                    if (!this.feedProducer.containsKey(fid)) {
                        System.out.println("Unknown Feed, ID: " + fid);
                    }
                JMSClientTopicConsumer consumer = new JMSClientTopicConsumer(ui.getURL(), ui.getViewName(), String.valueOf(ui.getId()));
                userConsumer.put(ui.getId(), consumer);
            }
        }
    }

    public HashMap<Integer,JMSClientQueueProducer> getFeedProducer(){
        return this.feedProducer;
    }

    public HashSet<UserInfo> getUsers(){
        return this.users;
    }

    public HashMap<Integer,JMSClientTopicConsumer> getUserConsumer(){
        return this.userConsumer;
    }
    
    public void SetPull(){
    	if(this.users != null && !this.users.isEmpty()){
    		for(UserInfo ui:this.users)
    			ui.setPull(ui.getURL(),ui.getViewName()+"_Pull");
    	}
    }

    public TopologyBuilder topologyBuilderPush(){
    	HashMap<View,ArrayList<View>> graph = simpleViews(true);
    	TopologyBuilder builder = wireTopology(graph,true);

        return builder;
    }

    public TopologyBuilder topologyBuilderPull(){
    	HashMap<View,ArrayList<View>> graph = simpleViews(false);
    	TopologyBuilder builder = wireTopology(graph,true);

        return builder;
    }
    private HashMap<View,ArrayList<View>> simpleViews(boolean isPush){
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        HashMap<Integer,View> feedViews = new HashMap<Integer,View>();
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            View v_fi = new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL());
            graph.put(v_fi, null);
            feedViews.put(fi.getId(),v_fi);
        }
        for(UserInfo ui:users){
        	ArrayList<View> follow = new ArrayList<View>();
        	HashSet<String> answer = new HashSet<String>();
            View v_ui = new View(ui.getFollow(),"U"+String.valueOf(ui.getId()),false,ui.getURL());
            for(Integer feed:ui.getFollow()){
            	follow.add(feedViews.get(feed));
            	if(!isPush)
            		answer.add("F"+feed);
            }
            if(isPush){
            	v_ui.isMaterialized = true;
            	answer.add(v_ui.id);
            	ui.setQuery(answer);
            }else{
            	v_ui.isMaterialized = false;
            	ui.setQuery(answer);
            }
            ui.changePush(isPush);
            graph.put(v_ui, follow);
        }
        
//      for(View from:graph.keySet()) {
//      System.out.print(from.id + ":\t");
//      if(graph.get(from)!=null)
//      for (View to : graph.get(from)) {
//          System.out.print(to.id + "\t");
//      }
//      System.out.println();
//  }
//  System.out.println();
        
        
        return graph;
    }

    private HashMap<View,ArrayList<View>> combineViews(){
        //Transitive Reduction of View Graph
        HashMap<Integer, ArrayList<View>> vertex = new HashMap<Integer, ArrayList<View>>();
        HashSet<Integer> allFeeds = new HashSet<Integer>();
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        int max_size = 0;
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            if(vertex.containsKey(fi_set.size())) {
                vertex.get(fi_set.size()).add(new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL()));
            }else{
                ArrayList<View> vertex_group = new ArrayList<View>();
                vertex_group.add(new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL()));
                vertex.put(fi_set.size(),vertex_group);
                if(fi_set.size()>max_size)
                    max_size = fi_set.size();
            }
        }
        for(UserInfo ui:users){
            HashSet<Integer> ui_set = new HashSet<Integer>();
            ui_set.addAll(ui.getFollow());

            allFeeds.addAll(ui_set);
            if(vertex.containsKey(ui_set.size()))
                vertex.get(ui_set.size()).add(new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL()));
            else{
                ArrayList<View> vertex_group = new ArrayList<View>();
                vertex_group.add(new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL()));
                vertex.put(ui_set.size(),vertex_group);
                if(ui_set.size()>max_size)
                    max_size = ui_set.size();
            }
        }
        //Calculate the transitive closure
        for(Map.Entry<Integer,ArrayList<View>> entry:vertex.entrySet()){
            for(View view:entry.getValue()){
                if(!graph.containsKey(view)){
                    ArrayList<View> views = new ArrayList<View>();
                    graph.put(view,views);
                }
                for(int i = entry.getKey();i<=max_size;i++){
                    if(!vertex.containsKey(i))
                        continue;
                    for(View target:vertex.get(i)){
                        if(target.feeds.containsAll(view.feeds) && target != view && !target.isFeed)
                            graph.get(view).add(target);
                    }
                }
            }
        }
//
//        for(View from:graph.keySet()) {
//            if(from.isFeed)
//                System.out.print("F");
//            else
//                System.out.print("U");
//            System.out.print(from.id + ":\t");
//            for (View to : graph.get(from)) {
//                System.out.print(to.id + "\t");
//            }
//            System.out.println();
//        }
//        System.out.println();

        //Calculate the transitive reduction
        for(View fromV:graph.keySet()) {
            for (View midV : graph.keySet()) {
                if(midV == fromV || midV.feeds.size() <= fromV.feeds.size()||!graph.get(fromV).contains(midV))
                    continue;
                for (View toV : graph.keySet()) {
                    if (toV == midV || toV == fromV)
                        continue;
                    else if(!inPath(midV,toV,graph))
                        continue;
                    else{
                        if(graph.get(fromV).contains(toV))
                            graph.get(fromV).remove(toV);
                    }
                }
            }
        }
        
        HashMap<View,ArrayList<View>> fromgraph = new HashMap<View,ArrayList<View>>();
        for(Map.Entry<View,ArrayList<View>> e:graph.entrySet()){
        	for(View to:e.getValue()){
        		if(fromgraph.containsKey(to)){
        			fromgraph.get(to).add(e.getKey());
        		}else{
        			ArrayList<View> list = new ArrayList<View>();
        			list.add(e.getKey());
        			fromgraph.put(to, list);
        		}
        	}
        }
        
//        for(View from:graph.keySet()) {
//            if(from.isFeed)
//                System.out.print("F");
//            else
//                System.out.print("U");
//            System.out.print(from.id + ":\t");
//            for (View to : graph.get(from)) {
//                System.out.print(to.id + "\t");
//            }
//            System.out.println();
//        }
//        System.out.println();
        return fromgraph;
    }

    private boolean inPath(View midV, View toV,
			HashMap<View, ArrayList<View>> graph) {
		if(graph.get(midV).contains(toV))
			return true;
		else{
			if(graph.get(midV).isEmpty())
				return false;
			else{
				for(View v:graph.get(midV))
					return inPath(v,toV,graph);
				return false;
			}
		}
		
	}
    

	public TopologyBuilder topologyBuilderSharedPush(){
		HashMap<View,ArrayList<View>> graph = combineViews();
    	TopologyBuilder builder = wireTopology(graph,true);

        return builder;

    }


	
	
	public TopologyBuilder topologyBuilderPushThroughput(){
		HashMap<View,ArrayList<View>> graph = simpleViews(true);
//		TopologyBuilder builder = wireTopologyPassive(graph);
    	TopologyBuilder builder = wireTopology(graph,false);
//		TopologyBuilder builder = wireTopologyAlt(graph,false);
//		TopologyBuilder builder = wireTopologyMulti(graph);

        return builder;
    }

	public TopologyBuilder topologyBuilderPushThroughputPassive(){
		HashMap<View,ArrayList<View>> graph = simpleViews(true);
		TopologyBuilder builder = wireTopologyPassive(graph);
//    	TopologyBuilder builder = wireTopology(graph,false);
//		TopologyBuilder builder = wireTopologyAlt(graph,false);
//		TopologyBuilder builder = wireTopologyMulti(graph);

        return builder;
    }
	
    public TopologyBuilder topologyBuilderPullThroughput(){
    	HashMap<View,ArrayList<View>> graph = simpleViews(false);
//    	TopologyBuilder builder = wireTopologyPassive(graph);
    	TopologyBuilder builder = wireTopology(graph,false);
//    	TopologyBuilder builder = wireTopologyAlt(graph,false);
//    	TopologyBuilder builder = wireTopologyMulti(graph);
        return builder;
    }
    
    public TopologyBuilder topologyBuilderPullThroughputPassive(){
    	HashMap<View,ArrayList<View>> graph = simpleViews(false);
    	TopologyBuilder builder = wireTopologyPassive(graph);
//    	TopologyBuilder builder = wireTopology(graph,false);
//    	TopologyBuilder builder = wireTopologyAlt(graph,false);
//    	TopologyBuilder builder = wireTopologyMulti(graph);
        return builder;
    }
    
    //Passive feed use a global timer control the emit speed
    //VirtualJmsBolt and TimerSpout used
    private TopologyBuilder wireTopologyPassive(HashMap<View,ArrayList<View>> graph){
    	TopologyBuilder builder = new TopologyBuilder();
    	HashMap<String,BoltDeclarer> matView = new HashMap<String,BoltDeclarer>();
    	TimerSpout timer = new TimerSpout();
    	builder.setSpout("Timer", timer, Math.ceil(feeds.size() / 10 + users.size() / 10));
    	 //Materialize Feeds
        for(FeedInfo fi:feeds){
        		VirtualJmsBolt spout = new VirtualJmsBolt(fi.getId(), Simulator.spoutEmit, fi.getUpdateIntv() * Simulator.minStep);
        		builder.setBolt(String.valueOf(fi.getId()),spout).shuffleGrouping("Timer");
        		JmsBufferedBolt feedbuffer = new JmsBufferedBolt(SortByTimeBolt.TOPN,fi.getId());
        		BoltDeclarer bolt = builder.setBolt("MatF"+Long.valueOf(fi.getId()), feedbuffer).shuffleGrouping(String.valueOf(fi.getId()));
        		matView.put("F"+fi.getId(), bolt);
        }
       
       HashMap<String,BoltDeclarer> virtualView = new HashMap<String,BoltDeclarer>();
       //Processing Views
       for(View v:graph.keySet()){
    	   if(v.isMaterialized){
    		   if(v.isFeed){
    			   v.isProcessed = true;
    		   }else{
    			   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id));
    			   for(View from:graph.get(v)){
    				   if(from.isFeed)
    					   bolt.shuffleGrouping(from.id.substring(1));//Remove the leading "F"
    				   else
    					   bolt.shuffleGrouping("Aggregator-"+from.id);
    			   }
    			   JmsBufferedBolt viewbuffer = new JmsBufferedBolt(SortByTimeBolt.TOPN,v.feeds);
    			   BoltDeclarer matbolt = builder.setBolt("MatAggregator-" + v.id, viewbuffer).shuffleGrouping("Aggregator-"+v.id);
    			   matView.put(v.id, matbolt);
    	           v.isProcessed = true;	   
    		   }
    		 
    	   }else{
    		   //Virtual View, don't need to materialize
    		   //Should be consumed by user, and set related information after materialized view generation finish
    		   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id,v.feeds));
    		   v.isProcessed = true;
    		   virtualView.put(v.id, bolt);
    	   }
       }

       //For each user, specify the view that could answer the user query
       //And collect the performance metric
       BaseRichBolt resultBolt;
       BoltDeclarer resultDeclarer;
       resultBolt = new ThroughputCountBolt();
       resultDeclarer = builder.setBolt("Output",resultBolt);

        for(UserInfo ui: users){
        	int id = ui.getId();
//        	double pullint = ui.getpullInt();
        	int qfreq = ui.getRequestIntv();
        	if(ui.hasAlias()){
        		id = ui.getAlias();
        	}
            if(ui.isPush()){
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }else{
            	
            	HashSet<String> views = ui.getQuery();
            	BoltDeclarer agg = virtualView.get("U"+id);
            	NotifierBolt noti = new NotifierBolt(ui.getId(),qfreq);//Each user should have its own notifier
            	int refid = ui.getPullreference();
            	builder.setBolt("Notifier-U" + ui.getId(), noti).shuffleGrouping("Timer");
            	
            	for(String src:views){
            		if(src == null)
            			continue;
            		if(src.contains("F")){
            			agg.fieldsGrouping("Mat" + src,new Fields("UID"));
            		}else if(src.contains("U")){
            			agg.fieldsGrouping("MatAggregator-" + src, new Fields("UID"));
            		}else{
            			Log.error("Unknown source type " + src);
            		}
            		matView.get(src).shuffleGrouping("Notifier-U" + ui.getId()); // Link notifier to buffers
            	}
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }
        }
        
      JmsBolt jmsBolt = new JmsBolt();
      JMSClientTopicProducer topicProducer = new JMSClientTopicProducer(Simulator.mqURL, "OutputThroughput","OutputThroughput");
      jmsBolt.setJmsProvider(topicProducer.getProvider());
      jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());

      builder.setBolt("OutputThroughput", jmsBolt).shuffleGrouping("Output");

        return builder;
    }
    
    //Topology wire without JMS system
    //VirtualSpout used.
    private TopologyBuilder wireTopology(HashMap<View,ArrayList<View>> graph, boolean isLatency){
    	TopologyBuilder builder = new TopologyBuilder();
    	HashMap<String,BoltDeclarer> matView = new HashMap<String,BoltDeclarer>();
    	 //Materialize Feeds
        for(FeedInfo fi:feeds){
        		VirtualJmsSpout spout = new VirtualJmsSpout(fi.getId(), Simulator.spoutEmit, Integer.valueOf((int) Math.floor(Simulator.minStep * fi.getUpdateIntv())));
        		builder.setSpout(String.valueOf(fi.getId()),spout);
        		JmsBufferedBolt feedbuffer = new JmsBufferedBolt(SortByTimeBolt.TOPN,fi.getId());
        		BoltDeclarer bolt = builder.setBolt("MatF"+Long.valueOf(fi.getId()), feedbuffer).shuffleGrouping(String.valueOf(fi.getId()));
        		matView.put("F"+fi.getId(), bolt);
        }
       
       HashMap<String,BoltDeclarer> virtualView = new HashMap<String,BoltDeclarer>();
       //Processing Views
       for(View v:graph.keySet()){
    	   if(v.isMaterialized){
    		   if(v.isFeed){
    			   v.isProcessed = true;
    		   }else{
    			   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id));
    			   for(View from:graph.get(v)){
    				   if(from.isFeed)
    					   bolt.shuffleGrouping(from.id.substring(1));//Remove the leading "F"
    				   else
    					   bolt.shuffleGrouping("Aggregator-"+from.id);
    			   }
    			   JmsBufferedBolt viewbuffer = new JmsBufferedBolt(SortByTimeBolt.TOPN,v.feeds);
    			   BoltDeclarer matbolt = builder.setBolt("MatAggregator-" + v.id, viewbuffer).shuffleGrouping("Aggregator-"+v.id);
    			   matView.put(v.id, matbolt);
    	           v.isProcessed = true;	   
    		   }
    		 
    	   }else{
    		   //Virtual View, don't need to materialize
    		   //Should be consumed by user, and set related information after materialized view generation finish
    		   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id,v.feeds));
    		   v.isProcessed = true;
    		   virtualView.put(v.id, bolt);
    	   }
       }

       //For each user, specify the view that could answer the user query
       //And collect the performance metric
       BaseRichBolt resultBolt;
       BoltDeclarer resultDeclarer;
       if(isLatency){
    	   resultBolt = new LatencyCollectBolt(25000);
       }else{
    	   resultBolt = new ThroughputCountBolt();
       }
       resultDeclarer = builder.setBolt("Output",resultBolt);

        for(UserInfo ui: users){
        	int id = ui.getId();
        	double pullint = ui.getpullInt();
        	if(ui.hasAlias()){
        		id = ui.getAlias();
        	}
            if(ui.isPush()){
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }else{
            	
            	HashSet<String> views = ui.getQuery();
            	BoltDeclarer agg = virtualView.get("U"+id);
            	NotifierBolt noti = new NotifierBolt(ui.getId(),pullint);//Each user should have its own notifier
            	int refid = ui.getPullreference();
            	builder.setBolt("Notifier-U" + ui.getId(), noti).shuffleGrouping(String.valueOf(refid));
            	
            	for(String src:views){
            		if(src == null)
            			continue;
            		if(src.contains("F")){
            			agg.fieldsGrouping("Mat" + src,new Fields("UID"));
            		}else if(src.contains("U")){
            			agg.fieldsGrouping("MatAggregator-" + src, new Fields("UID"));
            		}else{
            			Log.error("Unknown source type " + src);
            		}
            		matView.get(src).shuffleGrouping("Notifier-U" + ui.getId()); // Link notifier to buffers
            	}
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }
        }
        
      JmsBolt jmsBolt = new JmsBolt();
      JMSClientTopicProducer topicProducer = new JMSClientTopicProducer(Simulator.mqURL, "OutputThroughput","OutputThroughput");
      jmsBolt.setJmsProvider(topicProducer.getProvider());
      jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());

      builder.setBolt("OutputThroughput", jmsBolt).shuffleGrouping("Output");

        return builder;
    }
    
  //Topology wire without JMS system
    //VirtualSpout used.
    //Indipendent notifier
    private TopologyBuilder wireTopologyAlt(HashMap<View,ArrayList<View>> graph, boolean isLatency){
    	TopologyBuilder builder = new TopologyBuilder();
    	HashMap<String,BoltDeclarer> matView = new HashMap<String,BoltDeclarer>();
    	 //Materialize Feeds
        for(FeedInfo fi:feeds){
        		VirtualJmsSpout spout = new VirtualJmsSpout(fi.getId(), Simulator.spoutEmit, Integer.valueOf((int) Math.floor(Simulator.minStep * fi.getUpdateIntv())));
        		builder.setSpout(String.valueOf(fi.getId()),spout);
        		JmsBufferedBolt feedbuffer = new JmsBufferedBolt(SortByTimeBolt.TOPN,fi.getId());
//        		BoltDeclarer bolt = builder.setBolt("MatF"+Long.valueOf(fi.getId()), feedbuffer, Math.ceil(30 / fi.getUpdateIntv())).allGrouping(String.valueOf(fi.getId()));
        		BoltDeclarer bolt = builder.setBolt("MatF"+Long.valueOf(fi.getId()), feedbuffer).allGrouping(String.valueOf(fi.getId()));
        		matView.put("F"+fi.getId(), bolt);
        }
       
       HashMap<String,BoltDeclarer> virtualView = new HashMap<String,BoltDeclarer>();
       //Processing Views
       for(View v:graph.keySet()){
    	   if(v.isMaterialized){
    		   if(v.isFeed){
    			   v.isProcessed = true;
    		   }else{
    			   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id));
    			   for(View from:graph.get(v)){
    				   if(from.isFeed)
    					   bolt.shuffleGrouping(from.id.substring(1));//Remove the leading "F"
    				   else
    					   bolt.shuffleGrouping("Aggregator-"+from.id);
    			   }
    			   JmsBufferedBolt viewbuffer = new JmsBufferedBolt(SortByTimeBolt.TOPN,v.feeds);
//    			   BoltDeclarer matbolt = builder.setBolt("MatAggregator-" + v.id, viewbuffer,(int)Math.ceil(v.updateFreq*20)).allGrouping("Aggregator-"+v.id);
    			   BoltDeclarer matbolt = builder.setBolt("MatAggregator-" + v.id, viewbuffer).allGrouping("Aggregator-"+v.id);
    			   matView.put(v.id, matbolt);
    	           v.isProcessed = true;	   
    		   }
    		 
    	   }else{
    		   //Virtual View, don't need to materialize
    		   //Should be consumed by user, and set related information after materialized view generation finish
    		   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id,v.feeds));
    		   v.isProcessed = true;
    		   virtualView.put(v.id, bolt);
    	   }
       }

       //For each user, specify the view that could answer the user query
       //And collect the performance metric
       BaseRichBolt resultBolt;
       BoltDeclarer resultDeclarer;
       if(isLatency){
    	   resultBolt = new LatencyCollectBolt(25000);
       }else{
    	   resultBolt = new ThroughputCountBolt();
       }
       resultDeclarer = builder.setBolt("Output",resultBolt);

        for(UserInfo ui: users){
        	int id = ui.getId();
//        	double pullint = ui.getpullInt();
        	int requestInt = ui.getRequestIntv();
        	if(ui.hasAlias()){
        		id = ui.getAlias();
        	}
            if(ui.isPush()){
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }else{
            	
            	HashSet<String> views = ui.getQuery();
            	BoltDeclarer agg = virtualView.get("U"+id);
            	NotifierSpout noti = new NotifierSpout(ui.getId(),requestInt);
            	int refid = ui.getPullreference();
            	builder.setSpout("Notifier-U" + ui.getId(), noti);
//            	builder.setBolt("Notifier-U" + ui.getId(), noti).shuffleGrouping(String.valueOf(refid));
            	
            	for(String src:views){
            		if(src == null)
            			continue;
            		if(src.contains("F")){
            			agg.fieldsGrouping("Mat" + src,new Fields("UID"));
            		}else if(src.contains("U")){
            			agg.fieldsGrouping("MatAggregator-" + src, new Fields("UID"));
            		}else{
            			Log.error("Unknown source type " + src);
            		}
            		matView.get(src).shuffleGrouping("Notifier-U" + ui.getId()); // Link notifier to buffers
            	}
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }
        }
        
      JmsBolt jmsBolt = new JmsBolt();
      JMSClientTopicProducer topicProducer = new JMSClientTopicProducer(Simulator.mqURL, "OutputThroughput","OutputThroughput");
      jmsBolt.setJmsProvider(topicProducer.getProvider());
      jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());

      builder.setBolt("OutputThroughput", jmsBolt).shuffleGrouping("Output");

        return builder;
    }
    
  //Topology wire without JMS system
    //VirtualSpout used.
    //Indipendent notifier
    //Only spout emit count collected for throughput statistic
    private TopologyBuilder wireTopologyMulti(HashMap<View,ArrayList<View>> graph){
    	TopologyBuilder builder = new TopologyBuilder();
    	HashMap<String,BoltDeclarer> matView = new HashMap<String,BoltDeclarer>();
    	 //Materialize Feeds
        for(FeedInfo fi:feeds){
        		VirtualJmsSpout spout = new VirtualJmsSpout(fi.getId(), Simulator.spoutEmit, Integer.valueOf((int) Math.floor(Simulator.minStep * fi.getUpdateIntv())));
        		builder.setSpout(String.valueOf(fi.getId()),spout);
        		JmsMultiBufferedBolt feedbuffer = new JmsMultiBufferedBolt(fi.getId());
        		BoltDeclarer bolt = builder.setBolt("MatF"+Long.valueOf(fi.getId()), feedbuffer).allGrouping(String.valueOf(fi.getId()));
        		matView.put("F"+fi.getId(), bolt);
        }
       
       HashMap<String,BoltDeclarer> virtualView = new HashMap<String,BoltDeclarer>();
       //Processing Views
       for(View v:graph.keySet()){
    	   if(v.isMaterialized){
    		   if(v.isFeed){
    			   v.isProcessed = true;
    		   }else{
    			   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new MultiSortByTimeBolt("Aggregator-" + v.id));
    			   for(View from:graph.get(v)){
    				   if(from.isFeed)
    					   bolt.shuffleGrouping(from.id.substring(1));//Remove the leading "F"
    				   else
    					   bolt.shuffleGrouping("Aggregator-"+from.id);
    			   }
    			   JmsMultiBufferedBolt viewbuffer = new JmsMultiBufferedBolt(v.feeds);
    			   BoltDeclarer matbolt = builder.setBolt("MatAggregator-" + v.id, viewbuffer).allGrouping("Aggregator-"+v.id);
    			   matView.put(v.id, matbolt);
    	           v.isProcessed = true;	   
    		   }
    		 
    	   }else{
    		   //Virtual View, don't need to materialize
    		   //Should be consumed by user, and set related information after materialized view generation finish
    		   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new MultiSortByTimeBolt("Aggregator-" + v.id,v.feeds));
    		   v.isProcessed = true;
    		   virtualView.put(v.id, bolt);
    	   }
       }

       //For each user, specify the view that could answer the user query
       //And collect the performance metric
       BaseRichBolt resultBolt = new ThroughputCountMultiBolt();
       BoltDeclarer resultDeclarer;
       resultDeclarer = builder.setBolt("Output",resultBolt);

        for(UserInfo ui: users){
        	int id = ui.getId();
        	int requestInt = ui.getRequestIntv();
        	if(ui.hasAlias()){
        		id = ui.getAlias();
        	}
            if(ui.isPush()){
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }else{
            	
            	HashSet<String> views = ui.getQuery();
            	BoltDeclarer agg = virtualView.get("U"+id);
            	NotifierSpout noti = new NotifierSpout(ui.getId(),requestInt);
            	int refid = ui.getPullreference();
            	builder.setSpout("Notifier-U" + ui.getId(), noti);
            	
            	for(String src:views){
            		if(src == null)
            			continue;
            		if(src.contains("F")){
            			agg.fieldsGrouping("Mat" + src,new Fields("UID"));
            		}else if(src.contains("U")){
            			agg.fieldsGrouping("MatAggregator-" + src, new Fields("UID"));
            		}else{
            			Log.error("Unknown source type " + src);
            		}
            		matView.get(src).shuffleGrouping("Notifier-U" + ui.getId()); // Link notifier to buffers
            	}
            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
            }
        }
        
      JmsBolt jmsBolt = new JmsBolt();
      JMSClientTopicProducer topicProducer = new JMSClientTopicProducer(Simulator.mqURL, "OutputThroughput","OutputThroughput");
      jmsBolt.setJmsProvider(topicProducer.getProvider());
      jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());

      builder.setBolt("OutputThroughput", jmsBolt).shuffleGrouping("Output");

        return builder;
    }
    
//    private TopologyBuilder wireTopology(HashMap<View,ArrayList<View>> graph, boolean isVirtual, boolean isLatency){
//    	TopologyBuilder builder = new TopologyBuilder();
//    	
//    	
//    	HashMap<String,JmsProvider> matviewProvider = new HashMap<String, JmsProvider>();
//    	 //Materialize Feeds
//        for(FeedInfo fi:feeds){
//        	if(isVirtual){
//        		VirtualJmsSpout spout = new VirtualJmsSpout(Long.valueOf(fi.getId()), Simulator.spoutEmit, Integer.valueOf((int) Math.floor(1000 * fi.getUpdateFreq())));
//        		builder.setSpout(String.valueOf(fi.getId()),spout);
//        		JmsBolt jmsBolt = new JmsBolt();
//                jmsBolt.setJmsProvider(feedProducer.get(fi.getId()).getProvider());
//                matviewProvider.put("F"+fi.getId(), feedProducer.get(fi.getId()).getProvider());
//                jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());
//
//                builder.setBolt("F" + String.valueOf(fi.getId()), jmsBolt).shuffleGrouping(String.valueOf(fi.getId()));
//        	}else{
//        		JmsSpout spout = new JmsSpout();
//                spout.setJmsProvider(feedProducer.get(fi.getId()).getProvider());
//                matviewProvider.put("F"+fi.getId(), feedProducer.get(fi.getId()).getProvider());
//                spout.setJmsTupleProducer(feedProducer.get(fi.getId()).getTupleProducer());
//                spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
//                spout.setDistributed(true);
//
//                builder.setSpout(String.valueOf(fi.getId()),spout);
//        	} 
//        }
//       
//       HashMap<String,BoltDeclarer> virtualView = new HashMap<String,BoltDeclarer>();
//       
//       for(View v:graph.keySet()){
//    	   if(v.isMaterialized){
//    		   if(v.isFeed){
//    			   v.isProcessed = true;
//    		   }else{
//    			   BoltDeclarer bolt = builder.setBolt("Aggregator-" + v.id,new SortByTimeBolt("Aggregator-" + v.id));
//    			   for(View from:graph.get(v)){
//    				   if(from.isFeed)
//    					   bolt.shuffleGrouping(from.id.substring(1));
//    				   else
//    					   bolt.shuffleGrouping("Aggregator-"+from.id);
//    			   }
//    			   JMSClientQueueProducer topicProducer = new JMSClientQueueProducer(v.URL,"View-"+v.id,v.id);
//    	             JmsBolt matView = new JmsBolt();
//    	             matView.setJmsProvider(topicProducer.getProvider());
//    	             matviewProvider.put(v.id, topicProducer.getProvider());
//    	             matView.setJmsMessageProducer(new JMSClientMessageProducer());
//    	             builder.setBolt("View-"+v.id,matView).shuffleGrouping("Aggregator-" + v.id);
//    	             v.isProcessed = true;	   
//    		   }
//    		 
//    	   }else{
//    		   //Virtual View, don't need to materialize
//    		   //Should be consumed by user, and set related information after materialized view generation finish
//    		   BoltDeclarer bolt = builder.setBolt("PullAggregator-" + v.id,new SortByTimeBolt("PullAggregator-" + v.id));
//    		   v.isProcessed = true;
//    		   virtualView.put(v.id, bolt);
//    	   }
//       }
//
//       //For each user, specify the view that could answer the user query
//       //And collect the performance metric
//       BaseRichBolt resultBolt;
//       BoltDeclarer resultDeclarer;
//       if(isLatency){
//    	   resultBolt = new LatencyCollectBolt(100);
//       }else{
//    	   resultBolt = new ThroughputCountBolt();
//       }
//       resultDeclarer = builder.setBolt("Output",resultBolt);
//
//        
//        HashSet<String> pullViews = new HashSet<String>();
//        for(UserInfo ui: users){
//        	int id = ui.getId();
//        	if(ui.hasAlias())
//        		id = ui.getAlias();
//            if(ui.isPush()){
//            	resultDeclarer.shuffleGrouping("Aggregator-U"+id);
//            }else{
//            	
//            	HashSet<String> views = ui.getQuery();
//            	BoltDeclarer agg = virtualView.get("U"+id);
//            	for(String src:views){
//            		if(!pullViews.contains(src)){
//            			PullSpout ps = new PullSpout();
//            			ps.setInterval((int)Math.ceil(1000 * ui.getRequestFreq()));
//            			ps.setJmsProvider(matviewProvider.get(src));
//            			ps.setJmsTupleProducer(new JsonTupleProducer());
//            			ps.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
//            			
//            			builder.setSpout("Pull-" + src, ps);
//            			pullViews.add(src);
//            			
//            		}
//            		agg.shuffleGrouping("Pull-" + src);
//            		
//            	}
//            	resultDeclarer.shuffleGrouping("PullAggregator-U"+id);
//            }
//        }
//       
//        JmsBolt jmsBolt = new JmsBolt();
//        JMSClientTopicProducer topicProducer = new JMSClientTopicProducer(Simulator.mqURL, "OutputThroughput","OutputThroughput");
//        jmsBolt.setJmsProvider(topicProducer.getProvider());
//        jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());
//
//        builder.setBolt("OutputThroughput", jmsBolt).shuffleGrouping("Output");
//
//        return builder;
//    }
    
	public TopologyBuilder topologyBuilderSharedPushThroughput(){
        HashMap<View,ArrayList<View>> graph = this.combineViews();
        return this.wireTopology(graph, false);
    }
	
	public TopologyBuilder topologyBuilderGreedy(){
		HashMap<View,ArrayList<View>> graph = this.greedyViews();
//		return this.wireTopologyMulti(graph);
//		return this.wireTopologyAlt(graph,false);
		return this.wireTopology(graph,false);
//		return this.wireTopologyPassive(graph);
	}
	
	public TopologyBuilder topologyBuilderGreedyPassive(){
		HashMap<View,ArrayList<View>> graph = this.greedyViews();
//		return this.wireTopologyMulti(graph);
//		return this.wireTopologyAlt(graph,false);
//		return this.wireTopology(graph,false);
		return this.wireTopologyPassive(graph);
	}

	public TopologyBuilder topologyBuilderHierachyGreedy(){
		HashMap<View,ArrayList<View>> graph = this.hierachyGreedyViews();
		return this.wireTopology(graph,false);
//		return this.wireTopologyPassive(graph);
	}
	
	public TopologyBuilder topologyBuilderHierachyGreedyPassive(){
		HashMap<View,ArrayList<View>> graph = this.hierachyGreedyViews();
//		return this.wireTopology(graph,false);
		return this.wireTopologyPassive(graph);
	}
	
	public HashMap<View,ArrayList<View>> hierachyGreedyViews(){
		HashMap<Integer, HashSet<View>> vertex = new HashMap<Integer, HashSet<View>>();
        HashMap<String, View> viewmap = new HashMap<String,View>();
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        HashMap<Integer,Double> feedFreq = new HashMap<Integer,Double>();
        int max_size = 0;
        //int totalview = feeds.size();
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            feedFreq.put(fi.getId(), 1 / Double.valueOf(fi.getUpdateIntv()));
            View nv = new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL());
            nv.isMaterialized = true;
            if(vertex.containsKey(fi_set.size())) {     	
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.queryFreq = 0;
                vertex.get(fi_set.size()).add(nv);
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.queryFreq = 0;
                vertex_group.add(nv);
                vertex.put(fi_set.size(),vertex_group);
                if(fi_set.size()>max_size)
                    max_size = fi_set.size();
            }
            viewmap.put(nv.id, nv);
            this.debugMatViewCount++;
        }
        
        for(UserInfo ui:users){
            HashSet<Integer> ui_set = new HashSet<Integer>();
            ui_set.addAll(ui.getFollow());
            this.debugAverageFollow += ui.getFollow().size();
        	View nv = new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL());
            if(vertex.containsKey(ui_set.size())){
            	//Combine duplicate views
            	if(vertex.get(ui_set.size()).contains(nv)){
            		for(View n:vertex.get(ui_set.size())){
            			if(n.equals(nv)){
            				n.queryFreq += 1 / Double.valueOf(ui.getRequestIntv());
            				n.setAlias(ui.getId());
            				ui.setAlias(Integer.valueOf(n.id.substring(1)));
            				this.debugShareViews++;
            				break;
            			}
            		}
            	}else{
            		nv.updateFreq = 0;
            		for(Integer f:ui.getFollow())
            			nv.updateFreq += feedFreq.get(f);
            		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
            		vertex.get(ui_set.size()).add(nv);
            		viewmap.put(nv.id, nv);
            	}
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 0;
        		for(Integer f:ui.getFollow())
        			nv.updateFreq += feedFreq.get(f);
        		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
                vertex_group.add(nv);
                vertex.put(ui_set.size(),vertex_group);
                if(ui_set.size()>max_size)
                    max_size = ui_set.size();
                viewmap.put(nv.id, nv);
            }
        }
        
        if(vertex.keySet().contains(null)){
        	System.out.println("null vertex");
        }
        
        if(viewmap.containsKey(null)){
        	System.out.println("null view");
        }
        
        this.debugAllViewCount = viewmap.size();
        //Calculate the transitive closure
        for(Map.Entry<Integer,HashSet<View>> entry:vertex.entrySet()){
            for(View view:entry.getValue()){
                if(!graph.containsKey(view)){
                    ArrayList<View> views = new ArrayList<View>();
                    graph.put(view,views);
                }
                for(int i = entry.getKey();i<=max_size;i++){
                    if(!vertex.containsKey(i))
                        continue;
                    for(View target:vertex.get(i)){
                        if(target.feeds.containsAll(view.feeds) && target != view && !target.isFeed)
                            graph.get(view).add(target);
                    }
                }
            }
        }
        
        //Display the generated graph
//      for(View from:graph.keySet()) {
//	      System.out.print(from.id + ":\t");
//	      for (View to : graph.get(from)) {
//	          System.out.print(to.id + "\t");
//	      }
//	      System.out.println();
//      }
//      System.out.println();
//      System.out.println();
        
        //Candidate view generation finished
        
        if(graph.containsKey(null))
        	System.out.println("null key");
        
        //Reverse the adjacency matrix
        HashMap<View,ArrayList<View>> fromgraph = new HashMap<View,ArrayList<View>>();
        for(Map.Entry<View,ArrayList<View>> e:graph.entrySet()){
        	if(!fromgraph.containsKey(e.getKey())){
	        	ArrayList<View> elist = new ArrayList<View>();
	        	fromgraph.put(e.getKey(), elist);
        	}
        	for(View to:e.getValue()){
        		if(to == null)
        			continue;
        		if(fromgraph.containsKey(to)){
        			fromgraph.get(to).add(e.getKey());
        		}else{
        			ArrayList<View> list = new ArrayList<View>();
        			list.add(e.getKey());
        			fromgraph.put(to, list);
        		}
        	}
        }
        
      //DEBUG
//        for(View from:fromgraph.keySet()) {
//      	  if(from.isMaterialized)
//      		  System.out.print("(M)");
//  	      System.out.print(from.id + ":\t");
//  	      if(fromgraph.get(from) == null){
//  	    	  System.out.println();
//  	    	  continue;
//  	      }
//  	      for (View to : fromgraph.get(from)) {
//  	          System.out.print(to.id + "\t");
//  	      }
//  	      System.out.println();
//        }
//        System.out.println();
        
        //Greedy Materialization Algorithm
        //1. Calculate the materialization order using DFS on fromgraph
        ArrayList<View> candidateViews = new ArrayList<View>();
        HashSet<View> visitedView = new HashSet<View>();
        Stack<View> dfs = new Stack<View>();
        for(View v:vertex.get(max_size)){
        	dfs.push(v);
        	while(!dfs.isEmpty()){
        		View t = dfs.peek();
        		if(visitedView.contains(t)){
        			dfs.pop();
        			candidateViews.add(t);
        		}else{
        			visitedView.add(t);
        			if(!fromgraph.get(t).isEmpty())
	        			for(View from:fromgraph.get(t))
	        				dfs.push(from);
        		}
        	}
        }
        int submax = max_size - 1;
        while(visitedView.size() != viewmap.size()){
        	if(!vertex.containsKey(submax)){
        		submax--;
        		continue;
        	}
        		
        	for(View v:vertex.get(submax)){
            	dfs.push(v);
            	while(!dfs.isEmpty()){
            		View t = dfs.peek();
            		if(visitedView.contains(t)){
            			dfs.pop();
            			if(!candidateViews.contains(t))
            				candidateViews.add(t);
            		}else{
            			visitedView.add(t);
            			for(View from:fromgraph.get(t))
            				dfs.push(from);
            		}
            	}
            }
        	submax--;
        }
        //2. Initialize the view selection plan as all pull from feeds
        HashMap<View,ArrayList<View>> returngraph = new HashMap<View,ArrayList<View>>();
        for(UserInfo ui:users){
        	String id = String.valueOf(ui.getId());
        	if(!viewmap.containsKey("U"+String.valueOf(ui.getId()))){
        		id = String.valueOf(ui.getAlias());
        	}
        	ArrayList<View> follows = new ArrayList<View>();
        	for(int fi:ui.getFollow())
        		follows.add(viewmap.get("F"+fi));
        	returngraph.put(viewmap.get("U"+id), follows);
        }
        
        //3. Calculate whether an view should be materialized in bottom-up order
        HashSet<String> materialized = new HashSet<String>();
        for(FeedInfo fi:feeds){
        	materialized.add("F"+fi.getId());
        	returngraph.put(viewmap.get("F"+fi.getId()), new ArrayList<View>());
        }
        for(View tomat:candidateViews){
        	if(tomat.isFeed)
        		continue;
        	if(!viewmap.containsKey(tomat.id))
        		System.out.println("Error, unknown view: " + tomat.id);
        	if(viewmap.get(tomat.id).isProcessed)
        		continue;
        	else
        		viewmap.get(tomat.id).isProcessed = true;
        	if(decision(returngraph,viewmap.get(tomat.id),materialized,fromgraph,graph)){
        		viewmap.get(tomat.id).isMaterialized = true;
        		materialized.add(tomat.id);
        		newMaterialized(returngraph,viewmap.get(tomat.id),materialized,fromgraph,graph,viewmap);//Add tomat to returngraph, edit all the views that could benefit from tomat
        	}else{
        		tomat.isMaterialized = false;//Add tomat to returngraph, calculate the minimum set cover for tomat.feeds()
        		newVirtual(returngraph,viewmap.get(tomat.id),materialized,fromgraph,true);
        	}
        }
        this.debugMatViewCount = materialized.size();
        
        //4. Assign generated view selection plan to each user's query
        int raw_size = 0;
        int new_size = 0;
        //4. Assign generated view selection plan to each user's query
        for(UserInfo ui:users){
        	int id = ui.getId();
        	if(!viewmap.containsKey("U"+String.valueOf(ui.getId()))){
        		id = ui.getAlias();
        	}
        	HashSet<String> sources = new HashSet<String>();
        	
        	if(viewmap.get("U"+String.valueOf(id)).isMaterialized){
        		ui.setPush(true);
        		sources.add("U"+String.valueOf(id));
        	}else{
//        		HashMap<String,String> pullViews = new HashMap<String,String>();
//        		if(!returngraph.containsKey(viewmap.get("U"+String.valueOf(id))) || returngraph.get(viewmap.get("U"+String.valueOf(id))) == null){
//        			for(Integer f:ui.getFollow())
//        				pullViews.put("F"+f, ui.getURL());
//        		}else{
//	        		for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
//	        			if(v == null)
//	        				continue;
//	            		pullViews.put(v.id, v.URL);
//	            	}
//        		}
        		HashMap<String,String> pullViews = new HashMap<String,String>();
        		raw_size += viewmap.get("U"+String.valueOf(id)).feeds.size();
        		new_size += viewmap.get("U"+String.valueOf(id)).links.size();
        		for(View v:viewmap.get("U"+String.valueOf(id)).links){
        			pullViews.put(v.id, v.URL);
        			sources.add(v.id);
        		}
//        		cost_pull += (1.0 / (double)ui.getRequestIntv()) * pullViews.size();
        		ui.setPull(pullViews);
        		if(pullViews.size() <= 1)
        			System.out.println("Pull from one view!");
        	}
        	ui.setQuery(sources);
        }
        
        System.out.println("Raw size: " + raw_size + "   New Size: " + new_size);
        
        //DEBUG
//        for(View from:returngraph.keySet()) {
//      	  if(from.isMaterialized)
//      		  System.out.print("(M)");
//  	      System.out.print(from.id + ":\t");
//  	      if(returngraph.get(from) == null){
//  	    	  System.out.println();
//  	    	  continue;
//  	      }
//  	      for (View to : returngraph.get(from)) {
//  	          System.out.print(to.id + "\t");
//  	      }
//  	      System.out.println();
//        }
//        System.out.println();
        
		return returngraph;
	}
	private int newVirtual(HashMap<View, ArrayList<View>> returngraph,
			View v, HashSet<String> materialized, HashMap<View, ArrayList<View>> fromgraph, boolean calc) {
		
		HashSet<Integer> allfollow = new HashSet<Integer>();
		ArrayList<View> candidateView = new ArrayList<View>();
		ArrayList<View> selectView = new ArrayList<View>();
		//List all materialized view that could answer part of v's query
		int size = fromgraph.get(v).size();
		for(View sourcev:fromgraph.get(v)){
			if(!materialized.contains(sourcev.id))
				continue;
			candidateView.add(sourcev);
		}
		//Remove all the materialized views which is wholly contained by other one.
		ArrayList<View> removeView = new ArrayList<View>();
		for(View matv:candidateView){
			for(View matv2:candidateView){
				if(matv2.id == matv.id)
					continue;
				else if(matv.feeds.containsAll(matv2.feeds)){
					removeView.add(matv2);
				}
			}
		}
		candidateView.removeAll(removeView);
		
		while(!allfollow.containsAll(v.feeds)){
			Collections.sort(candidateView, new viewSizeComparator());
			if(!allfollow.containsAll(candidateView.get(0).feeds)){
					allfollow.addAll(candidateView.get(0).feeds);
					selectView.add(candidateView.get(0));
			}
			candidateView.remove(0);
		}
		if(returngraph.get(v).size() >= selectView.size()){
			if(calc){
				returngraph.put(v, selectView);
				v.links.clear();
				v.links.addAll(selectView);
			}
			return returngraph.get(v).size() - selectView.size();
		}else
			return 0;
		
	}

	private void newMaterialized(HashMap<View, ArrayList<View>> returngraph,
			View view, HashSet<String> materialized, HashMap<View, ArrayList<View>> fromgraph, HashMap<View, ArrayList<View>> graph, HashMap<String, View> viewmap) {
		returngraph.put(view,returngraph.get(view)); //Update isMaterialized flag for view in returngraph
		for(View to:graph.get(view)){
			if(!viewmap.get(to.id).isMaterialized)
				newVirtual(returngraph,to,materialized,fromgraph,true);
		}
		
	}

	private boolean decision(HashMap<View, ArrayList<View>> returngraph,
			View view, HashSet<String> materialized, HashMap<View, ArrayList<View>> fromgraph, HashMap<View, ArrayList<View>> graph) {
		double uc_inc = view.updateFreq * Simulator.costratio;
		double ev_dec = 0;
		materialized.add(view.id);
		for(View to:graph.get(view)){
			if(!materialized.contains(to.id))
				ev_dec += to.queryFreq * newVirtual(returngraph,to,materialized,fromgraph,false);
		}
		materialized.remove(view.id);
		double ev_cost = view.links.size() * (view.queryFreq);
		
		return (uc_inc - ev_dec) < ev_cost;
	}

	public HashMap<View, ArrayList<View>> greedyViews() {
		HashMap<Integer, HashSet<View>> vertex = new HashMap<Integer, HashSet<View>>();
        HashMap<String, View> viewmap = new HashMap<String,View>();
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        HashMap<Integer,Double> feedFreq = new HashMap<Integer,Double>();
        int max_size = 0;
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            feedFreq.put(fi.getId(), 1 / Double.valueOf(fi.getUpdateIntv()));
            View nv = new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL());
            nv.isMaterialized = true;
            if(vertex.containsKey(fi_set.size())) {     	
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.queryFreq = 0;
                vertex.get(fi_set.size()).add(nv);
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.queryFreq = 0;
                vertex_group.add(nv);
                vertex.put(fi_set.size(),vertex_group);
                if(fi_set.size()>max_size)
                    max_size = fi_set.size();
            }
            viewmap.put(nv.id, nv);
            this.debugMatViewCount++;
        }
        for(UserInfo ui:users){
            HashSet<Integer> ui_set = new HashSet<Integer>();
            ui_set.addAll(ui.getFollow());
            this.debugAverageFollow += ui.getFollow().size();
        	View nv = new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL());
            if(vertex.containsKey(ui_set.size())){
            	//Combine duplicate views
            	if(vertex.get(ui_set.size()).contains(nv)){
            		for(View n:vertex.get(ui_set.size())){
            			if(n.equals(nv)){
            				n.queryFreq += 1 / Double.valueOf(ui.getRequestIntv());
            				n.setAlias(ui.getId());
            				ui.setAlias(Integer.valueOf(n.id.substring(1)));
            				this.debugShareViews++;
            				break;
            			}
            		}
            	}else{
            		nv.updateFreq = 0;
            		for(Integer f:ui.getFollow())
            			nv.updateFreq += feedFreq.get(f);
            		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
            		vertex.get(ui_set.size()).add(nv);
            		viewmap.put(nv.id, nv);
            	}
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 0;
        		for(Integer f:ui.getFollow())
        			nv.updateFreq += feedFreq.get(f);
        		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
                vertex_group.add(nv);
                vertex.put(ui_set.size(),vertex_group);
                if(ui_set.size()>max_size)
                    max_size = ui_set.size();
                viewmap.put(nv.id, nv);
            }
        }
        
        if(vertex.keySet().contains(null)){
        	System.out.println("null vertex");
        }
        
        if(viewmap.containsKey(null)){
        	System.out.println("null view");
        }
        
        this.debugAllViewCount = viewmap.size();
        //Calculate the transitive closure
        for(Map.Entry<Integer,HashSet<View>> entry:vertex.entrySet()){
            for(View view:entry.getValue()){
                if(!graph.containsKey(view)){
                    ArrayList<View> views = new ArrayList<View>();
                    graph.put(view,views);
                }
                for(int i = entry.getKey();i<=max_size;i++){
                    if(!vertex.containsKey(i))
                        continue;
                    for(View target:vertex.get(i)){
                        if(target.feeds.containsAll(view.feeds) && target != view && !target.isFeed)
                            graph.get(view).add(target);
                    }
                }
            }
        }
        //Candidate view generation finished
        
        //DEBUG
        //Display the generated graph
//      for(View from:graph.keySet()) {
//	      System.out.print(from.id + ":\t");
//	      for (View to : graph.get(from)) {
//	          System.out.print(to.id + "\t");
//	      }
//	      System.out.println();
//      }
//      System.out.println();
        if(graph.containsKey(null))
        	System.out.println("null key");
        
        //Reverse the adjacency matrix
        HashMap<View,ArrayList<View>> fromgraph = new HashMap<View,ArrayList<View>>();
        for(Map.Entry<View,ArrayList<View>> e:graph.entrySet()){
        	for(View to:e.getValue()){
        		if(fromgraph.containsKey(to)){
        			fromgraph.get(to).add(e.getKey());
        		}else{
        			ArrayList<View> list = new ArrayList<View>();
        			list.add(e.getKey());
        			fromgraph.put(to, list);
        		}
        	}
        }
        
        //Greedy Materialization Algorithm
        //1. Calculate the benefit cost ratio in descending order
        ArrayList<View> bpc = new ArrayList<View>();
        bpc.addAll(fromgraph.keySet());
        HashSet<String> materialized = new HashSet<String>();
        for(View v:bpc){
        	v.bpc =  v.queryFreq / (v.updateFreq*Simulator.costratio);
        	int linkcount;
        	if(v.links.isEmpty())
        		linkcount = v.feeds.size();
        	else
        		linkcount = v.links.size();
        	v.bpc *= linkcount;
        	
        	
        }
        while(true && !bpc.isEmpty()){
	        //Collections.sort(bpc);
	        View tomat = bpc.get(0);
	        if(tomat == null)
	        	System.out.println("null mat");
	        if(viewmap.get(tomat.id).bpc <= 1)
	        	break;
	        viewmap.get(tomat.id).isMaterialized = true;
	        materialized.add(tomat.id);
	        if(tomat.isFeed)
	        	break;
	        for(View v:graph.get(tomat)){
	        	if(viewmap.get(v.id).links.isEmpty()){
	        		viewmap.get(v.id).links.add(tomat);
	        		HashSet<Integer> allfollow = new HashSet<Integer>();
	        		allfollow.addAll(v.feeds);
	        		allfollow.removeAll(tomat.feeds);
	        		for(Integer f:allfollow){
	        			View tv = viewmap.get("F"+String.valueOf(f));
	        			viewmap.get(v.id).links.add(tv);
	        			if(tv == null)
	        				System.out.println("null feed1");
	        		}
	        	}else{
	        		HashSet<Integer> allfollow = new HashSet<Integer>();
	        		ArrayList<View> candidateView = new ArrayList<View>();
	        		//List all materialized view that could answer part of v's query
	        		for(View sourcev:fromgraph.get(v)){
	        			if(!viewmap.get(sourcev.id).isMaterialized)
	        				continue;
	        			candidateView.add(sourcev);
	        		}
	        		//Remove all the materialized views which is wholly contained by other one.
	        		ArrayList<View> removeView = new ArrayList<View>();
	        		for(View matv:candidateView){
	        			for(View matv2:candidateView){
	        				if(matv2.id == matv.id)
	        					continue;
	        				else if(matv.feeds.containsAll(matv2.feeds)){
	        					removeView.add(matv2);
	        				}
	        			}
	        		}
	        		candidateView.removeAll(removeView);
	        		
	        		//Greedy set cover, choosing biggest set first
	        		while(!allfollow.containsAll(v.feeds)){
	        			Collections.sort(candidateView, new viewSizeComparator());
	        			if(!allfollow.containsAll(candidateView.get(0).feeds)){
	        					allfollow.addAll(candidateView.get(0).feeds);
	        					View tv = viewmap.get(candidateView.get(0).id);
	        					viewmap.get(v.id).links.add(tv);
	        					if(tv == null)
	    	        				System.out.println("null feed2");
	        			}
	        			candidateView.remove(0);
	        		}
	        	}
	        	double tbpc = viewmap.get(v.id).queryFreq / (viewmap.get(v.id).updateFreq*Simulator.costratio);
	        	int linkcount;
	        	if(viewmap.get(v.id).links.isEmpty())
	        		linkcount = viewmap.get(v.id).feeds.size();
	        	else
	        		linkcount = viewmap.get(v.id).links.size();
	        	viewmap.get(v.id).bpc = tbpc * linkcount;
	        }
	        	
	        bpc.remove(0);
        }
        this.debugMatViewCount = materialized.size();
        
        HashMap<View,ArrayList<View>> returngraph = new HashMap<View,ArrayList<View>>();
        
        for(Map.Entry<String, View> e:viewmap.entrySet()){
        	if(e.getValue() == null)
        		System.out.println("Null View: " + e.getKey());
        	if(e.getValue().isFeed)
        		returngraph.put(e.getValue(), null);
        	else if(e.getValue().links.isEmpty()){
        		ArrayList<View> src = new ArrayList<View>();
        		for(Integer f:e.getValue().feeds){
        			src.add(viewmap.get("F"+f));
        		}
        		if(src.contains(null))
        			System.out.println("null source1");
        		returngraph.put(e.getValue(), src);
        	}else{
        		ArrayList<View> src = new ArrayList<View>();
        		src.addAll(e.getValue().links);
        		if(src.contains(null))
        			System.out.println("null source2");
        		returngraph.put(e.getValue(), src);
        	}
        }
        
        
        for(UserInfo ui:users){
        	int id = ui.getId();
        	if(!viewmap.containsKey("U"+String.valueOf(ui.getId()))){
        		id = ui.getAlias();
        	}
        	
        	if(viewmap.containsKey("U"+String.valueOf(id)) && viewmap.get("U"+String.valueOf(id)).isMaterialized)
        		ui.setPush(true);
        	else{
        		HashMap<String,String> pullViews = new HashMap<String,String>();
        		if(!returngraph.containsKey(viewmap.get("U"+String.valueOf(id))) || returngraph.get(viewmap.get("U"+String.valueOf(id))) == null){
        			for(Integer f:ui.getFollow())
        				pullViews.put("F"+f, ui.getURL());
        		}else{
	        		for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
	        			if(v == null)
	        				continue;
	            		pullViews.put(v.id, v.URL);
	            	}
        		}
        		ui.setPull(pullViews);
        	}
        	HashSet<String> sources = new HashSet<String>();
        	for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
        		if(v == null)
        			continue;
        		sources.add(v.id);
        	}
        	ui.setQuery(sources);
        }
        
        //DEBUG
        //Display the generated graph
//      for(View from:returngraph.keySet()) {
//    	  if(from.isMaterialized)
//    		  System.out.print("(M)");
//	      System.out.print(from.id + ":\t");
//	      if(returngraph.get(from) == null){
//	    	  System.out.println();
//	    	  continue;
//	      }
//	      for (View to : returngraph.get(from)) {
//	          System.out.print(to.id + "\t");
//	      }
//	      System.out.println();
//      }
//      System.out.println();
        
        
		return returngraph;
	}
	
	public void storeDataset(String filename) throws IOException{
    	File f = new File(filename);
    	while(f.exists()){
    		filename += "c";
    		f = new File(filename);
    	}
    	f.createNewFile();
    	FileOutputStream fs = new FileOutputStream(f);
    	PrintStream ps = new PrintStream(fs);
    	ps.println(this.feeds.size() + "\t" + this.users.size());
    	for(FeedInfo fi:feeds)
    		ps.println(fi.toStorage());
    	for(UserInfo ui:users)
    		ps.println(ui.toStorage());
    	ps.close();
    }
	
	public TopologyGenerator(String filename) throws IOException{
		this.feeds = new HashSet<FeedInfo>();
		this.users = new HashSet<UserInfo>();
		File f = new File(filename);
		if(!f.exists()){
			Log.error("Error reading file: " + filename);
			return;
		}
		FileInputStream fs = new FileInputStream(f);
		BufferedReader read = new BufferedReader(new InputStreamReader(fs));
		String line = read.readLine();
		int fcount = Integer.valueOf(line.split("\t")[0]);
		int ucount = Integer.valueOf(line.split("\t")[1]);
		int count = 0;
		while((line = read.readLine()) != null){
			count++;
			if(count <= fcount)
				this.feeds.add(new FeedInfo(line));
			else
				this.users.add(new UserInfo(line));
		}
		if(this.feeds.size() != fcount || this.users.size() != ucount)
			Log.error("Wrong number of info readed, feed:" + this.feeds.size() + "/" + fcount + ",user:" + this.users.size() + "/"+ucount);
	}

}

class viewSizeComparator implements Comparator<View>{

	@Override
	public int compare(View v1, View v2) {
		return v2.feeds.size() - v1.feeds.size();
	}
	
}

