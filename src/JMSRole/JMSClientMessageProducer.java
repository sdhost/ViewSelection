package JMSRole;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.tuple.Tuple;


import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import jline.internal.Log;

/**
 * Created by kaijichen on 9/22/14.
 * 
 * Need modification to use, currently wrong
 */
public class JMSClientMessageProducer implements JmsMessageProducer, Serializable{
    @Override
    public Message toMessage(Session session, Tuple tuple) throws JMSException {
//    	Log.info("MessageToQueue");
        if(tuple.getValue(0) instanceof String) {
            TextMessage tm = session.createTextMessage(tuple.getStringByField("json") + "\t" + tuple.getLongByField("timestamp") + "\t" + tuple.getLongByField("sendTime"));
//            tm.setLongProperty("timestamp", tuple.getLongByField("timestamp"));
            return tm;
        }else if(tuple.getValue(0) instanceof Queue){
            String msg = "";
            long timestamp = 0;
            for(Tuple tpl:(LinkedBlockingQueue<Tuple>)tuple.getValue(0)){
                msg += tpl.getStringByField("json") + "\t" + tpl.getLongByField("timestamp") + "\t" + tpl.getLongByField("sendTime") + "\n";
                timestamp = tpl.getLongByField("timestamp");
            }
            TextMessage tm = session.createTextMessage(msg);
//            tm.setLongProperty("timestamp", timestamp);
            return tm;
        }else if(tuple.getValue(0) instanceof Double){
            StringBuilder msg = new StringBuilder();
            msg.append(tuple.getDoubleByField("amount")).append("\t")
            											.append(tuple.getDoubleByField("throughput"))
            											.append("\t")
            											.append(tuple.getLongByField("timestamp"));
            TextMessage tm = session.createTextMessage(msg.toString());
            return tm;
        }else if (tuple.getValue(0) instanceof Integer){
        	String msg = String.valueOf(tuple.getIntegerByField("amount"));
            TextMessage tm = session.createTextMessage(msg);
            return tm;
        }else{
            TextMessage tm = session.createTextMessage("Unknow tuple: " + tuple.toString());
            tm.setJMSTimestamp(System.currentTimeMillis());
            return tm;
        }
    }
}
