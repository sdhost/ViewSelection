package bolt;

import java.util.Map;
import java.util.Random;

import util.TupleHelpers;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VirtualJmsBolt extends BaseRichBolt {

	OutputCollector _collector;
	Integer count;
	Integer msg = 0;
	Integer id;
	Integer freq_intv = 1;
	int counter = 0;
	
	public VirtualJmsBolt(Integer id, int count, Integer freq){
		this.count = count;
		this.id = id;
		this.freq_intv = freq;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if (TupleHelpers.isTickTuple(input)) {
            //LOG.debug("Received tick tuple, triggering emit of current window counts");
        }else{
			this.msg++;
			if(this.msg <= this.count){
				this.counter++;
				if(this.counter >= freq_intv){
					Long time = System.currentTimeMillis();
					this._collector.emit(input,new Values(String.valueOf(msg), time, this.id));
					this.counter = 0;
				}
				
			}else{
				//this._collector.emit(new Values(this.id,0,0));
			}
        }
		this._collector.ack(input);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json","timestamp","SID"));
	}

}
