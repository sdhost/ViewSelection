package bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import util.TupleData;
import util.TupleHelpers;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JmsMultiBufferedBolt extends BaseRichBolt {
	
	private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(JmsBufferedBolt.class);


	private HashSet<Integer> coverage;
	private ConcurrentHashMap<Integer,Integer> inputBuffer;
	
	private OutputCollector collector;
	
	public JmsMultiBufferedBolt(HashSet<Integer> coverage){
		this.coverage = new HashSet<Integer>();
		this.inputBuffer = new ConcurrentHashMap<Integer,Integer>();
		for(Integer f:coverage)
			this.inputBuffer.put(f, 0);
	}
	
	public JmsMultiBufferedBolt(Integer id){
		this.coverage = new HashSet<Integer>();
		this.coverage.add(id);
		this.inputBuffer = new ConcurrentHashMap<Integer,Integer>();
		this.inputBuffer.put(id, 0);
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
            if(tuple.getFields().size() == 2 && tuple.contains("UID")){
            	//From NotifierBolt
            	Integer uid = tuple.getIntegerByField("UID");
            	Integer serial = tuple.getIntegerByField("Serial");
            	emit(uid,serial);
            }else if(tuple.getFields().size() == 3){
            	//From VirtualJmsSpout
                buffer(tuple);
            }else if(tuple.getFields().size() == 2 && tuple.contains("PackedMessage")){
            	//From SortByTimeBolt
            	buffer((ConcurrentHashMap<Integer,Integer>)tuple.getValueByField("PackedMessage"));
            	
            }else{
            	LOG.error("Unknown tuple!");
            }
        }
        collector.ack(tuple);

	}

	private void buffer(ConcurrentHashMap<Integer, Integer> concurrentHashMap) {
		for(Map.Entry<Integer,Integer> e : concurrentHashMap.entrySet()){
			if(this.inputBuffer.containsKey(e.getKey()) && this.inputBuffer.get(e.getKey()) < e.getValue())
				this.inputBuffer.put(e.getKey(), e.getValue());
		}
	}

	private void buffer(Tuple tuple) {
		String json = tuple.getStringByField("json");
		Integer id = tuple.getIntegerByField("SID");
		if(this.inputBuffer.containsKey(id) && this.inputBuffer.get(id) < Integer.valueOf(json))
			this.inputBuffer.put(id, Integer.valueOf(json));
	}

	private void emit(Integer uid, Integer serial) {
		this.collector.emit(new Values(uid,serial,this.inputBuffer,this.coverage));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("UID","Serial","PackedMessage","Cover"));
	}

}
