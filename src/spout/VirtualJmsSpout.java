package spout;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

//VirtualJmsSpout worked as a message source in view selection simulation.
//It generate (json,timestamp,SID) tuple and send to JmsBufferedBolt (For Feed Message Materialization)
//SortByTimeBolt (For aggregation operation) and NotifierBolt (For pull notification) every §frequency time.
//It has a maximum number of tuple emitted limit §count, it will stop emit tuple when total emitted number of tuple
//reaches the limit.

public class VirtualJmsSpout extends BaseRichSpout implements Serializable{
	
	SpoutOutputCollector _collector;
	Random _rand;
	Integer count;
	Integer msg = 0;
	Integer id;
	Integer frequency = 1000;
	int gc_counter = 0;
	int gc_int = 1000000;
	
	public VirtualJmsSpout(Integer id, int count, Integer freq){
		this.count = count;
		this.id = id;
		this.frequency = freq;
	}
	
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;
//		this._rand = new Random(System.currentTimeMillis());
		
	}

	@Override
	public void nextTuple() {
		//TODO: Simulation assumption, for throughput statistic
//		if(frequency > 5000)
//			Utils.sleep((long)Math.ceil(Double.valueOf(frequency) / 1000d));
//		else{
//			long start = System.nanoTime();
//			long end = 0;
//			do{
//				end = System.nanoTime();
//			}while(start + frequency >= end);
//		}
		this.msg++;
		if(this.msg <= this.count){
			Long time = System.currentTimeMillis();
			this._collector.emit(new Values(String.valueOf(msg), time, this.id),new Values(this.id,this.msg));
//			this.gc_counter++;
//			if(this.gc_counter > this.gc_int){
//				this.gc_counter = 0;
//				System.gc();
//			}
		}else{
			//this._collector.emit(new Values(this.id,0,0));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json","timestamp","SID"));
		//INFO: In Throughput collection experiment, json is the message, timestamp is the message sending time at spout
		// and the sendTime will be the spout id that emit the message
	}

}
