package bolt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jline.internal.Log;
import util.TupleData;
import util.TupleHelpers;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MultiSortByTimeBolt extends BaseRichBolt implements Serializable{



	private static final long serialVersionUID = -3983919281349292326L;
	private Integer currentSerial = -1;
	private OutputCollector collector;
	
	private HashSet<Integer> feedSet;
	private HashSet<Integer> follow;
	
	private ConcurrentHashMap<Integer,Integer> aggdata;
	
	private String name;
	
//	private Long newest;
	
	public MultiSortByTimeBolt(String name) {
        this.name = name;
        this.aggdata = new ConcurrentHashMap<Integer,Integer>();
        this.feedSet = new HashSet<Integer>();
        this.follow = new HashSet<Integer>();
      }
	
	public MultiSortByTimeBolt(String name, HashSet<Integer> follows) {
        this.name = name;
        this.follow = follows;
        this.aggdata = new ConcurrentHashMap<Integer,Integer>();
        this.feedSet = new HashSet<Integer>();
      }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {

		}else{
		      if(tuple.size() == 3 && tuple.contains("json")){
		    	//Tuple from JmsSpout
		    	countObj(tuple);
		    	emit();
		      }else if(tuple.size() == 4 && tuple.contains("UID")){
		    	//Tuple from JmsBufferedBolt
		    	  Integer serial = tuple.getIntegerByField("Serial");
		    	  Integer uid = tuple.getIntegerByField("UID");
		    	  HashSet<Integer> cover = (HashSet<Integer>) tuple.getValueByField("Cover");

		    	    
		    	  if(serial > this.currentSerial){
		    		  if(!this.feedSet.isEmpty()){
		    			  emit();
		    		  }		    			  
		    		  resetStatues(serial);
		    	  }
		    	  countObj((ConcurrentHashMap<Integer,Integer>)tuple.getValueByField("PackedMessage"));
		    	  this.feedSet.addAll(cover);
    			  if(this.feedSet.containsAll(follow)){
    				  emit();
    				  resetStatues(0);
    			  }
		      }else if(tuple.size() == 2){
		    	  //Tuple from other SortByTimeBolt
		    	  countObj((ConcurrentHashMap<Integer,Integer>)tuple.getValueByField("PackedMessage"));
		    	  emit();
		      }else{
		    	  Log.error("Unknow tuple send to SortByTimeBolt : " + tuple.toString());
		      }
		    }
		collector.ack(tuple);
	}

	private void countObj(ConcurrentHashMap<Integer, Integer> concurrentHashMap) {
		for(Map.Entry<Integer,Integer> e : concurrentHashMap.entrySet()){
			if(!this.aggdata.containsKey(e.getKey()) || this.aggdata.get(e.getKey()) < e.getValue())
				this.aggdata.put(e.getKey(), e.getValue());
			if(!this.follow.contains(e.getKey()))
				this.follow.add(e.getKey());
		}
		
	}

	private void resetStatues(Integer serial) {
		this.feedSet.clear();
	}

	private void emit() {
		this.collector.emit(new Values(this.aggdata,System.currentTimeMillis()));
	}

	private void countObj(Tuple tuple) {
		String json = tuple.getStringByField("json");
		Integer id = tuple.getIntegerByField("SID");
		if(!this.aggdata.containsKey(id) || this.aggdata.get(id) < Integer.valueOf(json))
			this.aggdata.put(id, Integer.valueOf(json));
		if(!this.follow.contains(id))
			this.follow.add(id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("PackedMessage","timestamp"));

	}

}
