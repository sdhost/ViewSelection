package spout;

import java.util.Map;

import jline.internal.Log;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;

public class PullSpout extends JmsSpout {

	private int interval = 0;
	public void setInterval(int intveral){
		this.interval = interval;
	}

	@Override
	public void nextTuple() {
//		Log.info("PullSpout");
		super.nextTuple();
		Utils.sleep(interval);

	}

}
