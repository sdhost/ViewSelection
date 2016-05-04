package spout;

import java.util.Map;

import ViewSelection.Simulator;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class NotifierSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	private Integer uid;
	private Integer pullInt;
	private Integer serial = 0;
	
	public NotifierSpout(Integer uid, int requestInt){
		this.uid = uid;
		this.pullInt = requestInt;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;

	}

	@Override
	public void nextTuple() {
		final long INTERVAL = Simulator.minStep * pullInt;
		long start = System.nanoTime();
		long end = 0;
		do{
			end = System.nanoTime();
		}while(start + INTERVAL >= end);

		_collector.emit(new Values(uid,serial),new Values("U",this.uid,this.serial));
    	serial++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("UID","Serial"));
	}

}
