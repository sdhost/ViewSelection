/**
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
package Debug;

import backtype.storm.Config;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;
import bolt.JmsBufferedBolt;
import bolt.SortByTimeBolt;
import util.JmsNotificationTupleProducer;
import JMSRole.*;
import util.StormRunner;

import javax.jms.Session;

import java.io.Serializable;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingNewestFeed implements Serializable{

  private static final int DEFAULT_RUNTIME_IN_SECONDS = 180;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  private static JMSClientQueueProducer producer1;
  private static JMSClientQueueProducer producer2;
  private static JMSClientQueueProducer notifier;
  private static JMSClientTopicProducer topicProducer;
  private static JMSClientTopicConsumer topicConsumer;

  public RollingNewestFeed() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "RollingNewestFeed";
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    //wireTopology();
      wiretopologyPull();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    //conf.setDebug(true);
    conf.setDebug(false);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spout1Id = "Feed1";
    String spout2Id = "Feed2";
    String aggregatorId = "Aggregator";
    String topicId = "Topic";

    String URL = "tcp://54.216.210.86:61616";//ActiveMQ URL
    String queueName1 = "Queue1";//Queue1 for Feed1
    String queueName2 = "Queue2";//Queue2 for Feed2
    String topicName = "Topic";



    producer1 = new JMSClientQueueProducer(URL,queueName1,spout1Id);
    producer2 = new JMSClientQueueProducer(URL,queueName2,spout2Id);



    JmsSpout spout1 = new JmsSpout();
    spout1.setJmsProvider(producer1.getProvider());
    spout1.setJmsTupleProducer(producer1.getTupleProducer());
    spout1.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout1.setDistributed(true);

    JmsSpout spout2 = new JmsSpout();
    spout2.setJmsProvider(producer2.getProvider());
    spout2.setJmsTupleProducer(producer2.getTupleProducer());
    spout2.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
    spout2.setDistributed(true);


    builder.setSpout(spout1Id, spout1, 5);
    builder.setSpout(spout2Id, spout2, 5);

    builder.setBolt(aggregatorId+"0", new SortByTimeBolt(aggregatorId+"0"))
            .shuffleGrouping(spout1Id)
            .shuffleGrouping(spout2Id);
      int layer = 3;
      for(int i=0;i<layer;i++){
          builder.setBolt(aggregatorId+String.valueOf(i+1), new SortByTimeBolt(aggregatorId+String.valueOf(i+1)))
                  .shuffleGrouping(aggregatorId + String.valueOf(i));
      }


    JmsBolt jmsBolt = new JmsBolt();
    topicProducer = new JMSClientTopicProducer(URL, topicName, topicId);
    jmsBolt.setJmsProvider(topicProducer.getProvider());
    jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());

    builder.setBolt(topicId + "Output", jmsBolt).allGrouping(aggregatorId+String.valueOf(layer));

    //topicConsumer = new JMSClientTopicConsumer(URL, topicName, "TopicConsumer");


  }

  private void wiretopologyPull() throws InterruptedException{
      String spout1Id = "Feed1";
      String spout2Id = "Feed2";
      String spoutClientId = "Client";
      String aggregatorId = "Aggregator";
      String bufferId = "Buffer";
      String topicId = "Topic";

      String URL = "tcp://54.216.210.86:61616";//ActiveMQ URL
      String queueName1 = "Queue1";//Queue1 for Feed1
      String queueName2 = "Queue2";//Queue2 for Feed2
      String queueClient = "QueueClient";
      String topicName = "Topic";



      producer1 = new JMSClientQueueProducer(URL,queueName1,spout1Id);
      producer2 = new JMSClientQueueProducer(URL,queueName2,spout2Id);
      notifier = new JMSClientQueueProducer(URL, queueClient,spoutClientId);

      JmsSpout spout1 = new JmsSpout();
      spout1.setJmsProvider(producer1.getProvider());
      spout1.setJmsTupleProducer(producer1.getTupleProducer());
      spout1.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
      spout1.setDistributed(true);

      JmsSpout spout2 = new JmsSpout();
      spout2.setJmsProvider(producer2.getProvider());
      spout2.setJmsTupleProducer(producer2.getTupleProducer());
      spout2.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
      spout2.setDistributed(true);


      builder.setSpout(spout1Id, spout1, 5);
      builder.setSpout(spout2Id, spout2, 5);

      JmsSpout clientSpout = new JmsSpout();
      clientSpout.setJmsProvider(notifier.getProvider());
      clientSpout.setJmsTupleProducer(new JmsNotificationTupleProducer());
      clientSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
      clientSpout.setDistributed(false);

      builder.setSpout(spoutClientId, clientSpout, 1);


      builder.setBolt(bufferId + "1", new JmsBufferedBolt(TOP_N,1))
              .shuffleGrouping(spout1Id)
              .shuffleGrouping(spoutClientId);

      builder.setBolt(bufferId + "2", new JmsBufferedBolt(TOP_N,2))
              .shuffleGrouping(spout2Id)
              .shuffleGrouping(spoutClientId);

      builder.setBolt(aggregatorId+"0", new SortByTimeBolt(aggregatorId+"0"))
              .shuffleGrouping(bufferId + "1")
              .shuffleGrouping(bufferId + "2");
      int layer = 0;

      JmsBolt jmsBolt = new JmsBolt();
      topicProducer = new JMSClientTopicProducer(URL, topicName, topicId);
      jmsBolt.setJmsProvider(topicProducer.getProvider());
      jmsBolt.setJmsMessageProducer(new JMSClientMessageProducer());

      builder.setBolt(topicId + "Output", jmsBolt).shuffleGrouping(aggregatorId+String.valueOf(layer));
  }

  public void run() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public static void main(String[] args) throws Exception {
    new RollingNewestFeed().run();
  }
}
