/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package util;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.Serializable;

import jline.internal.Log;

/**
 * A simple <code>JmsTupleProducer</code> that expects to receive
 * JMSRole <code>TextMessage</code> objects with a body in JSON format.
 * <p/>
 * Ouputs a tuple with field name "json" and a string value
 * containing the raw json.
 * <p/>
 * <b>NOTE: </b> Currently this implementation assumes the text is valid
 * JSON and does not attempt to parse or validate it.
 * 
 * @author tgoetz
 *
 */
@SuppressWarnings("serial")
public class JsonTupleProducer implements JmsTupleProducer, Serializable {

	public Values toTuple(Message msg) throws JMSException {
//		Log.info("MessageToTuple");
		if(msg instanceof TextMessage){		
			String line = ((TextMessage) msg).getText();
			String[] elem = line.split("\t");
			String json = elem[0];
            Long timestamp = Long.valueOf(elem[1]);
            Long sendTime = Long.valueOf(elem[2]);
			return new Values(json,timestamp,sendTime);
		} else {
			return null;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json","timestamp","sendTime"));
	}

}
