package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import util.TupleHelpers;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Map;

import jline.internal.Log;

/**
 * Created by kaijichen on 11/3/14.
 * 
 * LatencyCollectBolt take message from SortByTimeBolt and calculate the latency using current time minus
 * the message send time. It will emit the latency of each message received with a timestamp of receiving
 * the message. To limit the emit speed, we use a counter, only emit the average latency after every window
 * size messages.
 */
public class LatencyCollectBolt extends BaseRichBolt implements Serializable{
	private static final long serialVersionUID = -7874743271624720580L;
	private OutputCollector collector;

	LinkedList<Long> latency;
	Integer window;
	Integer counter = 0;
	final Integer toEmit = 5000;//After how many messages received to emit a new statistic message.
							   // It should be smaller than windows size to make all the change collected.
	

    public LatencyCollectBolt(Integer window){
    	this.latency = new LinkedList<Long>();
    	this.window = window;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
        	
        }
        else {
            if(tuple.size() == 2){
            	//Message from SortByTimeBolt
            	this.counter++;
            	long current = System.currentTimeMillis();
                long lat = current - tuple.getLongByField("timestamp");
                if(this.latency.size() < this.window)
                	this.latency.offer(lat);
                else{
                	this.latency.poll();
                	this.latency.offer(lat);
                }
                if(this.counter >= this.toEmit){
//                	emit(current);
                	this.counter = 0;
                }
            }else{
            	Log.error("Unknown type of tuple send to LatencyCollectBolt : " + tuple.toString());
            }
        }
        collector.ack(tuple);
    }


    private void emit(long current) {
		Double lat = 0d;
		for(long i:this.latency)
			lat += i;
		lat /= this.latency.size();
		Log.info(lat);
		collector.emit(new Values(lat,current));
		
	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("amount","timestamp"));
    }
}
