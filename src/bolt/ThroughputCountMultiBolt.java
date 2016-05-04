package bolt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jline.internal.Log;
import util.TupleHelpers;
import ViewSelection.Simulator;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ThroughputCountMultiBolt extends BaseRichBolt implements Serializable {


	private static final long serialVersionUID = -6092709697839882064L;
	private OutputCollector collector;
	private Map<Integer,Integer> steps;
	private Long last = 0l;
	private Long begin = 0l;

	private Integer last_count = 0;
	
	private int INTERVAL = 10000;
	private Integer counter = 0;
	
	public ThroughputCountMultiBolt(){
		this.steps = new HashMap<Integer,Integer>();
//		if(Simulator.type == 0)
//			this.INTERVAL = 500;
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
            if(tuple.size() == 2){
//            	Tuple from SortByTimeBolt (PackedMessage,timestamp)
//            	this.counter++;
            	long current = System.currentTimeMillis();
            	if(this.last == 0){
            		this.last = current;
            		this.begin = current;
            	}else
            		countThroughput((ConcurrentHashMap<Integer,Integer>)tuple.getValueByField("PackedMessage"));
           
                if(current - last >= 1000) {
//                	Double thro = this.getThroughput(current);
                	Double count = Double.valueOf(this.getCount());
	                collector.emit(new Values(count,current));
//                	counter = 0;
	                last = current;
                }
            }else{
            	Log.error("Unknown type of tuple send to LatencyCollectBolt : " + tuple.toString());
            }
        }
		collector.ack(tuple);	

	}



	private int getCount() {
		Integer amount = 0;
		for(Map.Entry<Integer, Integer> e:steps.entrySet()){
			amount += e.getValue();
		}
		return amount;
	}

	private Double getThroughput(long current) {
		Integer amount = getCount();

		Double throughput = Double.valueOf(amount - this.last_count) / Double.valueOf(current - this.last) * 1000.0;
		this.last_count = amount;
		this.last = current;
		
		return Math.ceil(throughput);
	}

	private void countThroughput(ConcurrentHashMap<Integer, Integer> concurrentHashMap) {
		for(Map.Entry<Integer,Integer> e : concurrentHashMap.entrySet()){
			if(!this.steps.containsKey(e.getKey()) || this.steps.get(e.getKey()) < e.getValue())
				this.steps.put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("amount","timestamp"));
	}

}
