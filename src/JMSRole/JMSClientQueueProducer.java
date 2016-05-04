package JMSRole; /**
 * Created by kaijichen on 9/16/14.
 */

import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.Serializable;
import util.JsonTupleProducer;

public class JMSClientQueueProducer implements Runnable, Serializable{
    private String ID = null;
    private String ActiveMQURL = null;
    private String QueueName = null;
    private TextMessage nextMessage = null;
    private boolean isRunning = false;
    private int returnCode = -1;
    private JMSClientProvider provider = null;
    private JmsTupleProducer tupleProducer = null;

    public JMSClientQueueProducer(String URL, String QueueName, String ID){
        this.ActiveMQURL = URL;
        this.QueueName = QueueName;
        this.ID = ID;
        try {
            this.provider = new JMSClientProvider(URL, this.QueueName, true, false, Session.AUTO_ACKNOWLEDGE);
        }catch (Exception e){
            System.out.println("Caught: " + e);
            this.returnCode = 2;
            e.printStackTrace();
        }
        this.tupleProducer = new JsonTupleProducer();
    }

    public void setMessage(String text) throws JMSException{
        //String timestamp = String.valueOf(System.currentTimeMillis());
        //String tmpMessage = timestamp + "\t" + text;
        this.nextMessage = this.provider.getSession().createTextMessage(text);
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

    public JmsProvider getProvider(){
        return this.provider;
    }
    public void run() {
        isRunning = true;
        if(this.nextMessage == null) {
            this.returnCode = 1;
            isRunning = false;
            return;
        }

        try {
            MessageProducer producer = provider.getProducer(DeliveryMode.NON_PERSISTENT);

            // Tell the producer to send the message
            System.out.println("Sent message: "+ this.nextMessage.getText() + " : " + Thread.currentThread().getName());
            producer.send(this.nextMessage);
            this.nextMessage = null;

            // Clean up
            provider.close();
            this.returnCode = 0;
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            this.returnCode = 2;
            e.printStackTrace();
        }finally{
            isRunning = false;
        }
    }

    public void send(){
        try {
            MessageProducer producer = provider.getProducer(DeliveryMode.NON_PERSISTENT);

            // Tell the producer to send the message
            //System.out.println("Sent message: "+ this.nextMessage.getText() + " : " + Thread.currentThread().getName());
            producer.send(this.nextMessage);
            this.nextMessage = null;
        }
        catch (Exception e) {
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
