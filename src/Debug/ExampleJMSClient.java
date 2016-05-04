package Debug;

import javax.jms.TextMessage;

import JMSRole.*;

/**
 * Created by kaijichen on 9/22/14.
 */
public class ExampleJMSClient {

    public static void main(String[] args) throws Exception {
        String URL = "tcp://localhost:61616";//ActiveMQ URL
        String queueName1 = "Queue1";//Queue1 for Feed1
        String queueName2 = "Queue2";//Queue2 for Feed2
        String topicName = "Topic";
        JMSClientQueueProducer queueProducer1 = new JMSClientQueueProducer(URL, queueName1, "Feed1");
        JMSClientQueueProducer queueProducer2 = new JMSClientQueueProducer(URL, queueName2, "Feed2");
        JMSClientTopicProducer topicProducer = new JMSClientTopicProducer(URL, topicName, "TopicProducer");
        JMSClientQueueConsumer queueConsumer1 = new JMSClientQueueConsumer(URL, queueName1, "User1");
        JMSClientQueueConsumer queueConsumer2 = new JMSClientQueueConsumer(URL, queueName2, "User2");
        JMSClientTopicConsumer topicConsumer = new JMSClientTopicConsumer(URL, topicName, "TopicConsumer");

        queueProducer1.setMessage("TestQueue1");
        Thread t = new Thread(queueProducer1);
        t.run();

        while(t.isAlive()){
            Thread.sleep(500);
        }

        Thread s = new Thread(queueConsumer1);
        s.run();

        while(s.isAlive()){
            Thread.sleep(500);
        }

        TextMessage tm = queueConsumer1.getMessage();
        System.out.println(tm.toString());

    }
}
