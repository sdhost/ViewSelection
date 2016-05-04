package JMSRole;

import javax.jms.*;
import java.io.Serializable;

/**
 * Created by kaijichen on 9/16/14.
 */
public class JMSClientTopicConsumer implements Runnable, ExceptionListener, Serializable{
    private String ID = null;
    private TextMessage receivedMessage = null;
    private String ActiveMQURL = null;
    private String TopicName = null;
    private boolean isRunning = false;
    private int returnCode = -1;
    private final int lifetime = 10000;
    private JMSClientProvider provider = null;

    public JMSClientTopicConsumer(String URL, String topicName, String ID){
        this.ActiveMQURL = URL;
        this.TopicName = topicName;
        this.ID = ID;
        try {
            //Durable Consumer
            //this.provider = new JMSClientProvider(URL, this, this.TopicName, this.ID, false, false, Session.AUTO_ACKNOWLEDGE);

            //Non-Durable Consumer
            this.provider = new JMSClientProvider(URL, this, this.TopicName, false, false, Session.AUTO_ACKNOWLEDGE);
        }catch (Exception e){
            System.out.println("Caught: " + e);
            returnCode = 2;
            e.printStackTrace();
        }
    }
    public TextMessage getMessage(){
        if(this.receivedMessage != null)
            return this.receivedMessage;
        else
            return null;
    }

    public boolean isRun(){
        if (this.isRunning) {
            return true;
        }
        else return false;
    }
    public JMSClientProvider getProvider(){
        return this.provider;
    }

    public int getReturnCode(){
        return this.returnCode;
    }

    public void run()
    {
        this.isRunning = true;
        try
        {
            System.out.println("SimpleTopicConsumer, started on " + TopicName);

            MessageConsumer consumer = this.provider.getConsumer();


            long startTime = System.currentTimeMillis();

            while (true)
            {
                long now = System.currentTimeMillis();
                if (now - startTime > lifetime)
                {
                    System.out.println("Time's up, exiting...");
                    break;
                }

                // Wait for a message
                Message message = consumer.receive(1000);

                if (message == null)
                    continue;

                if (message instanceof TextMessage)
                {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("SimpleTopicConsumer - Received (text): " + text);
                    returnCode = 0;
                    break;
                }
                else
                {
                    System.out.println("SimpleTopicConsumer - Received: " + message);
                    returnCode = 3;
                }
            }

            consumer.close();
            provider.close();
        }
        catch (Exception e)
        {
            System.out.println("Caught: " + e);
            returnCode = 2;
            e.printStackTrace();
        }finally{
            this.isRunning = false;
        }

    }

    public synchronized void onException(JMSException ex)
    {
        System.out.println("JMSRole Exception occured.  Shutting down client.");
    }

    public TextMessage receive(){
        try
        {
            MessageConsumer consumer = this.provider.getConsumer();
            Message message = consumer.receive();
            if (message instanceof TextMessage)
                {
                    this.receivedMessage = (TextMessage) message;
                }
            else
                {
                    System.out.println("SimpleTopicConsumer - Received: " + message);
                }

            consumer.close();
        }
        catch (Exception e)
        {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
        return this.receivedMessage;
    }
    public TextMessage receiveDurable(){
        try
        {
            TopicSubscriber consumer = this.provider.getDurableSubscriber(ID);
            Message message = consumer.receive(1000);
            if (message instanceof TextMessage)
            {
                this.receivedMessage = (TextMessage) message;
            }
            else
            {
                //System.out.println("SimpleTopicConsumer - Received: " + message);
            }

            consumer.close();
        }
        catch (Exception e)
        {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
        return this.receivedMessage;
    }
    public void close(){
        try {
            this.provider.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
