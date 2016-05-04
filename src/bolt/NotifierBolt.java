/**
 * 
 */
package bolt;

import java.util.Map;

import jline.internal.Log;
import util.TupleHelpers;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author kaijichen
 * NotifierBolt take input from VirtualJmsSpout, only count the number of tuple received and send (UID,Serial)
 * to JmsBufferedBolt to activate a pull operation. Every user whose query are answered by pull strategy should
 * have an notifier and corresponding SortByTimeBolt to generate the query result.
 */
public class NotifierBolt extends BaseRichBolt {
	private Integer uid;
	private Double pullInt = -1d;
	private Integer request;
	private Integer overpull = 0;
	private Integer serial = 0;
	private int counter = 0;
	OutputCollector collector;
	
	public NotifierBolt(Integer uid, Double pullInt){
		this.uid = uid;
		this.pullInt = pullInt;
		if(pullInt < 1)
			this.overpull = (int) Math.ceil(1/pullInt);
	}
	
	public NotifierBolt(Integer uid, int requestInt){
		this.uid = uid;
		this.request = requestInt;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
            
        }
        else {
        	if(this.pullInt < 0){
        		this.counter++;
        		if(this.counter >= this.request){
        			collector.emit(tuple, new Values(uid,serial));
        			this.counter = 0;
        			serial++;
        		}
        	}
        	else if(tuple.size() == 3){
//            	Tuple from VirtualJmsSpout (json,timestamp,SID)
            	if(overpull == 0){
	            	this.counter++;
	                if(counter >= this.pullInt) {
		                collector.emit(tuple,new Values(uid,serial));
	                	counter = 0;
	                	serial++;
	                }
            	}else{
            		for(int i=0;i<overpull;i++){
            			collector.emit(tuple,new Values(uid,serial));
//            			final long INTERVAL = 100;
//            			long start = System.nanoTime();
//            			long end = 0;
//            			do{
//            				end = System.nanoTime();
//            			}while(start + INTERVAL >= end);
            			serial++;
            		}
            	}
            }else{
            	Log.error("Unknown type of tuple send to LatencyCollectBolt : " + tuple.toString());
            }
        }
		collector.ack(tuple);	

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("UID","Serial"));
	}

}
