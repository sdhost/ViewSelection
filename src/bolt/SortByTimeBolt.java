/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import org.apache.log4j.Logger;

import util.NewestTupleComparator;
import util.TupleData;
import util.TupleHelpers;

import java.io.Serializable;
import java.util.*;

import jline.internal.Log;

/*
 * SortByTimeBolt take input from JmsBufferedBolt or VirtualJmsSpout, and send message to other SortByTimeBolt
 * or metric collection bolt.
 * 1.VirtualJmsSpout (json,timestamp,SID), update the sliding window if the message is newer than held ones.
 * Then emit packed messages with newest tuple timestamp (PackedMessage,timestamp).
 * 2.SortByTimeBolt (PackedMessage,timestamp), if received message timestamp is newer than oldest tuple in counter,
 * unpack the message and update counter. Emit updated packed message with newest timestamp.
 * 3.JmsBufferedBolt (UID,Serial,PackedMessage,Cover), 
 * if Serial is smaller than current one, ignore tuple;
 * if Serial is the same with current one,
 * 		if corresponding feed is not updated, update counter and feedSet;
 * 			if feedSet is finished
 * 				emit(PackedMessage,timestamp)
 * 				empty counter
 * 				reset currentSerial to 0
 * 				reset feedSet
 * 		else ignore tuple;
 * else 
 * 	  emit(PackedMessage,timestamp) if counter is not empty, empty counter and feedSet, reset newestTime, update currentSerial.
 * 	  Update the counter and feedSet
 */

public class SortByTimeBolt extends BaseRichBolt implements Serializable {

  private static final long serialVersionUID = 5537727428628598519L;
  //private static final Logger LOG = Logger.getLogger(SortByTimeBolt.class);
  public static int TOPN = 10;
  private String name;
  private StringBuilder output;
  private String packedMessage;

  //private final PriorityQueue<TupleData> counter;
  //private final HashSet<TupleData> content;
  private final ArrayList<TupleData> counter;
  private HashSet<Integer> feedSet;
  private HashSet<Integer> follows;
  //private int receivedCount;
  private OutputCollector collector;

  private long lastEmit = -1;
  private long newestTime = 0;
  private Integer currentSerial;

  public SortByTimeBolt(String name, int topn) {
    //this.counter = new PriorityQueue<TupleData>(5,new newestTupleComparator());
    //this.content = new HashSet<TupleData>();
	  this.counter = new ArrayList<TupleData>();
    this.lastEmit = System.currentTimeMillis();
    TOPN = topn;
    this.name = name;
    this.output = new StringBuilder(TOPN*50);
  }

    public SortByTimeBolt(String name) {
//        this.counter = new PriorityQueue<TupleData>(5,new newestTupleComparator());
//        this.content = new HashSet<TupleData>();
        this.counter = new ArrayList<TupleData>();
        this.lastEmit = System.currentTimeMillis();
        this.name = name;
        this.output = new StringBuilder(TOPN*50);
    }
    
    public SortByTimeBolt(String name, HashSet<Integer> follows) {
    	this.counter = new ArrayList<TupleData>();
        this.lastEmit = System.currentTimeMillis();
        this.name = name;
        this.setVirtual(follows);
        this.output = new StringBuilder(TOPN*50);
      }
    
    public void setVirtual(HashSet<Integer> follows){
    	this.currentSerial = -1;
    	this.feedSet = new HashSet<Integer>();
    	this.follows = follows;
    	
    }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	  this.collector = collector;
  }
  
  private void resetStatues(int newSerial){
  //Reset uidMap, currentSerial and counter when a new serial number is arrived
	  this.counter.clear();
	  this.currentSerial = newSerial;
	  this.newestTime = 0;
	  feedSet.clear();
  }

  @Override
  public void execute(Tuple tuple) {
//	Log.info("Aggregator");
    if (TupleHelpers.isTickTuple(tuple)) {
      collector.ack(tuple);
    }
    else {
      if(tuple.size() == 3 && tuple.contains("json")){
    	//Tuple from JmsSpout
    	countObjAndAck(tuple);
    	emit(tuple,this.counter,newestTime);
      }else if(tuple.size() == 4 && tuple.contains("UID")){
    	//Tuple from JmsBufferedBolt
    	  Integer serial = tuple.getIntegerByField("Serial");
    	  Integer uid = tuple.getIntegerByField("UID");
    	  HashSet<Integer> cover = (HashSet<Integer>) tuple.getValueByField("Cover");

    	  
    	  if(serial == this.currentSerial){
    		  if(!feedSet.containsAll(cover)){
    			  String[] lines = tuple.getStringByField("PackedMessage").split("\n");
    			  for(String line:lines){
    		    		
    		    		countObj(line);
    			  }
    			  this.feedSet.addAll(cover);
    			  if(this.feedSet.containsAll(follows)){
    				  emit(tuple,this.counter,newestTime);
    				  resetStatues(0);
    			  }
    		  }
    	  }else if(serial > this.currentSerial){
    		  if(this.counter.isEmpty()){
    			  resetStatues(serial);
    			  String[] lines = tuple.getStringByField("PackedMessage").split("\n");
    			  for(String line:lines){
    		    		
    		    		countObj(line);
    			  }
    			  this.feedSet.addAll(cover);
    			  if(this.feedSet.containsAll(follows)){
    				  emit(tuple,this.counter,newestTime);
    				  resetStatues(0);
    			  }
    		  }else{
    			  emit(tuple,this.counter,newestTime);
    			  resetStatues(serial);
    			  String[] lines = tuple.getStringByField("PackedMessage").split("\n");
    			  for(String line:lines){
    		    		
    		    		countObj(line);
    			  }
    			  this.feedSet.addAll(cover);
    		  }
    	  }
      }else if(tuple.size() == 2){
    	  //Tuple from other SortByTimeBolt
    	  long time = tuple.getLongByField("timestamp");
    	  if(this.counter.isEmpty() || time > this.counter.get(0).timestamp){
    		  String[] lines = tuple.getStringByField("PackedMessage").split("\n");
			  for(String line:lines){
		    		
		    		countObj(line);
			  }
			  emit(tuple,this.counter,newestTime);
    	  }
      }else{
    	  Log.error("Unknow tuple send to SortByTimeBolt : " + tuple.toString());
      }
      collector.ack(tuple);
    }
  }


    private void emit(Tuple tuple,ArrayList<TupleData> counts, Long time) {
       //Pack message and emit with timestamp
    	this.output.delete(0, this.output.length());
    	for(TupleData tpl:counts)
    		output.append(tpl.json).append("\t")
			  .append(tpl.timestamp).append("\t")
			  .append(tpl.SID).append("\n");
    	this.packedMessage = this.output.toString();
        this.collector.emit(tuple,new Values(packedMessage,time));
    }

  private void countObjAndAck(Tuple tuple) {	
	//Update the counter if the message is newer than held ones.
	//Sort the counter to let oldest message in the frontier
	TupleData tmpTpl = new TupleData(tuple);
	if(counter.contains(tmpTpl)){
		return;
	}
	if(tuple.getLongByField("timestamp") > this.newestTime)
		this.newestTime = tuple.getLongByField("timestamp");
    if(counter.size() >= this.TOPN){
        if(tuple.getLongByField("timestamp").compareTo(counter.get(0).timestamp)>0) {
        	counter.get(0).copy(tuple);
        }
    }else{
    	counter.add(tmpTpl);
    }
    
    Collections.sort(this.counter, new NewestTupleComparator());
  }

    private void countObj(String line) {
    	//Update the counter if the message is newer than held ones.
    	//Sort the counter to let oldest message in the frontier
    	if(line.split("\t").length != 3)
    		return;
    	if(Long.valueOf(line.split("\t")[1]) > this.newestTime)
    		this.newestTime = Long.valueOf(line.split("\t")[1]);
    	TupleData tpldata = new TupleData(line);
    	if(counter.contains(tpldata)){
    		return;
    	}
        if(counter.size() >= this.TOPN){
            if(Long.valueOf(line.split("\t")[1]).compareTo(counter.get(0).timestamp)>0) {
            	this.counter.get(0).copy(line);
            }

        }else{
        	this.counter.add(tpldata);
        }
        Collections.sort(this.counter,new NewestTupleComparator());
    }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("PackedMessage","timestamp"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
  
}
