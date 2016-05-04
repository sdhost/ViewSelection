package JMSRole;

import backtype.storm.contrib.jms.JmsTupleProducer;

import javax.jms.*;
import java.io.Serializable;
import util.JsonTupleProducer;

/**
 * Created by kaijichen on 9/16/14.
 */
public class JMSClientTopicProducer implements Runnable, Serializable{
    private String ID = null;
    private TextMessage nextMessage = null;
    private String ActiveMQURL = null;
    private String TopicName = null;
    private boolean isRunning = false;
    private int returnCode = -1;
    private JMSClientProvider provider = null;
    private JmsTupleProducer tupleProducer = new JsonTupleProducer();

    public JMSClientTopicProducer(String URL, String topicName, String ID){
        this.ActiveMQURL = URL;
        this.TopicName = topicName;
        this.ID = ID;
        try {
            this.provider = new JMSClientProvider(this.ActiveMQURL, this.TopicName, false, false, Session.AUTO_ACKNOWLEDGE);
        }catch (Exception e){
            System.out.println("Caught: " + e);
            returnCode = 2;
            e.printStackTrace();
        }
    }
    public void setMessage(String text, Session session) throws JMSException {
        this.nextMessage = session.createTextMessage(text);

    }
    public JMSClientProvider getProvider(){
        return this.provider;
    }

    public boolean isRun(){
        if (this.isRunning) {
            return true;
        }
        else return false;
    }

    public int getReturnCode(){
        return this.returnCode;
    }

    public JmsTupleProducer getTupleProducer(){
    return this.tupleProducer;
    }
    public void run()
    {
        isRunning = true;
        if(this.nextMessage == null) {
            this.returnCode = 1;
            isRunning = false;
            return;
        }

        try
        {
            MessageProducer producer = provider.getProducer(DeliveryMode.NON_PERSISTENT);

            // Tell the producer to send the message
            System.out.println("Sent message: " + this.nextMessage.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(this.nextMessage);

            // Clean up
            provider.close();
            returnCode = 0;
        }
        catch (Exception e)
        {
            System.out.println("Caught: " + e);
            returnCode = 2;
            e.printStackTrace();
        }finally {
            isRunning = false;
        }
    }

    public void send(){
        try
        {
            MessageProducer producer = provider.getProducer(DeliveryMode.NON_PERSISTENT);

            // Tell the producer to send the message
            System.out.println("Sent message: " + this.nextMessage.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(this.nextMessage);
        }
        catch (Exception e)
        {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }
    public void close(){
        try {
            this.provider.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
