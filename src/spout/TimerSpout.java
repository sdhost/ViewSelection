package spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TimerSpout extends BaseRichSpout {

	private long tick = 0l;
	SpoutOutputCollector _collector;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;

	}

	@Override
	public void nextTuple() {
		this._collector.emit(new Values(tick),tick);
//		this._collector.emit(new Values(tick));
		tick++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tick"));

	}

}
