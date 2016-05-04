package bolt;

import java.io.Serializable;
import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import util.NewestTupleComparator;
import util.TupleData;
import util.TupleHelpers;

/**
 * Created by kaijichen on 9/26/14.
 * 
 * JmsBufferedBolt take input tuple from three kind of sources and store them into a constant size sliding window
 * 1. VirtualJmsSpout (json,timestamp,SID), just buffer the message.
 * 2. NotifierBolt (UID,Serial), pack all buffered messages into line-break string if inputBuffer is not empty, use the
 * packedMessage directly otherwise. Send packed message to corresponding aggregator using tuple format (UID,PackedMessage)
 * 3. SortByTimeBolt (PackedMessage,timestamp), update the packed message.
 */
public class JmsBufferedBolt extends BaseRichBolt implements Serializable{

    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(JmsBufferedBolt.class);

    private ArrayList<TupleData> inputBuffer;
    private int buffer_size;
    private String packedMessage;
    private StringBuilder output;
    private long lastUpdate;
    private HashSet<Integer> coverage;
    private int gc_counter = 0;
    private int GC_INT = 500000;

    private OutputCollector collector;

    public JmsBufferedBolt(int buffer_size, HashSet<Integer> coverage){
        this.inputBuffer = new ArrayList<TupleData>(buffer_size);
        this.buffer_size = buffer_size;
//        this.packedMessage = "";
        this.coverage = coverage;
        this.output = new StringBuilder(this.buffer_size * 50);
    }

    public JmsBufferedBolt(int buffer_size, Integer id) {
    	this.inputBuffer = new ArrayList<TupleData>(buffer_size);
        this.buffer_size = buffer_size;
//        this.packedMessage = "";
        this.coverage = new HashSet<Integer>();
        this.coverage.add(id);
        this.output = new StringBuilder();
	}

	@Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            //LOG.debug("Received tick tuple, triggering emit of current window counts");
        }
        else {
            if(tuple.getFields().size() == 2 && tuple.contains("UID")){
            	//From NotifierBolt
            	Integer uid = tuple.getIntegerByField("UID");
            	Integer serial = tuple.getIntegerByField("Serial");
            	if(this.inputBuffer.isEmpty()){
            		//Buffer bolt of an SortByTimeBolt
            		if(this.packedMessage != null)
            			this.collector.emit(new Values(uid,serial,packedMessage,this.coverage));
            	}else{
            		//Buffer bolt of an VirtualJmsSpout
            		emit(tuple,uid,serial);
            	}
            }else if(tuple.getFields().size() == 3){
            	//From VirtualJmsSpout
                buffer(tuple);
            }else if(tuple.getFields().size() == 2 && tuple.contains("PackedMessage")){
            	//From SortByTimeBolt
            	//this.packedMessage = tuple.getStringByField("PackedMessage");
            	this.packedMessage = tuple.getStringByField("PackedMessage");
            	this.lastUpdate = tuple.getLongByField("timestamp");
            	
            }else{
            	LOG.error("Unknown tuple!");
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("UID","Serial","PackedMessage","Cover"));
    }

//    private void emit(){
//        this.collector.emit(new Values(this.inputBuffer));
//    }

    private void emit(Tuple tuple,Integer uid, Integer serial){
    	if(this.inputBuffer.isEmpty())
    		return;
    	output.delete(0, output.length());
    	for(TupleData tpl:this.inputBuffer)
    		output.append(tpl.json).append("\t")
    			  .append(tpl.timestamp).append("\t")
    			  .append(tpl.SID).append("\n");
    	this.packedMessage = output.toString();
        this.collector.emit(tuple,new Values(uid,serial,packedMessage,this.coverage));
    }

    private void buffer(Tuple tuple){
    	lastUpdate = tuple.getLongByField("timestamp");
        int size = this.inputBuffer.size();
        if(size <= this.buffer_size) {
        	TupleData tpl = new TupleData(tuple);
            this.inputBuffer.add(tpl);
            if(this.inputBuffer.size() <= size)
                LOG.error("======================Error in add tuple to the buffer!!!============");
        }else{
        	
            this.inputBuffer.get(0).copy(tuple);
            Collections.sort(this.inputBuffer, new NewestTupleComparator());
        }
//        this.gc_counter++;
//        if(this.gc_counter > this.GC_INT){
//        	this.gc_counter = 0;
//        	System.gc();
//        }
    }
}
