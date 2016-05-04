package bolt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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

/**
 * Created by kaijichen on 11/3/14.
 * 
 * ThroughputCountBolt take message from SortByTimeBolt and calculate the processed message count from all feed,
 * it will emit the total number of message processed since last receive with the timestamp received it.
 */

public class ThroughputCountBolt extends BaseRichBolt implements Serializable{

	private static final long serialVersionUID = 1735476671671913294L;
	private OutputCollector collector;
	private Map<String,Integer> steps;
	private Integer counter = 0;
	private Long last = 0l;
	private Long begin = 0l;
	private Integer last_count = 0;
	private int INTERVAL = 1000;
	private int GC_INT = 100;
	private int gc_counter = 0;
//	private PrintStream ps;
	
	public ThroughputCountBolt(){
		this.steps = new HashMap<String,Integer>();
//		File f = new File("Log"+System.currentTimeMillis());
//    	try {
//			f.createNewFile();
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//    	FileOutputStream fos = null;
//		try {
//			fos = new FileOutputStream(f);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//    	ps = new PrintStream(fos);
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
            	this.counter++;
            	long current = System.currentTimeMillis();
            	if(this.last == 0){
            		begin = current;
            		this.last = current;
            	}
            	countThroughput(tuple);
           
                if(current - last >= 1000) {
                	Double thro = this.getThroughput(current);
                	Double count = Double.valueOf(this.getCount());
//	                Log.info(count,current);
//	                ps.println(thro + "\t" + current);
//	                collector.emit(new Values(thro,current));
	                collector.emit(tuple,new Values(count,thro,current));
//                	Log.info(this.getCount() + "\t" + String.valueOf(current - this.last));
                	counter = 0;
                	this.last = current;
//                	this.gc_counter++;
//                	if(this.gc_counter > this.GC_INT){
//                		this.gc_counter = 0;
//                		System.gc();
//                	}
                	
                }
            }else{
            	Log.error("Unknown type of tuple send to LatencyCollectBolt : " + tuple.toString());
            }
        }
		collector.ack(tuple);	
	}

	private Double getThroughput(long current) {
		Integer amount = getCount();

		Double throughput = Double.valueOf(amount - this.last_count) / Double.valueOf(current - this.last) * 1000.0;
		this.last_count = amount;
		this.last = current;
		
		return Math.ceil(throughput);
	}
	
	private int getCount() {
		Integer amount = 0;
//		String line = "";
		for(Map.Entry<String, Integer> e:steps.entrySet()){
			amount += e.getValue();
//			line += "Feed-"+e.getKey() + ":" + e.getValue() + "\t";
		}
//		Log.info(line);
		return amount;
	}

	private void countThroughput(Tuple tuple) {
		String lines = tuple.getStringByField("PackedMessage");
		for(String line:lines.split("\n")){
			String[] elem = line.split("\t");
			if(elem.length != 3){
				Log.error(line);
				Log.error(lines.length());
				Log.error(((StringBuilder)tuple.getValueByField("PackedMessage")).toString());
			}
			Integer count = Integer.valueOf(elem[0]);
			String id = elem[2];
			if(this.steps.containsKey(id)){
				if(this.steps.get(id) < count)
					this.steps.put(id, count);
			}else{
				this.steps.put(id,count);
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("amount","throughput","timestamp"));
	}

}
