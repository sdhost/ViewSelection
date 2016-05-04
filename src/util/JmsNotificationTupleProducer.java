package util;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.Serializable;

/**
 * Created by kaijichen on 9/29/14.
 */
public class JmsNotificationTupleProducer implements JmsTupleProducer, Serializable {
    @Override
    public Values toTuple(Message msg) throws JMSException {
        if(msg instanceof TextMessage){
            String json = "Pull";
            Long time = System.currentTimeMillis();
            return new Values(json,time);
        } else {
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json","sendTime"));
    }
}
