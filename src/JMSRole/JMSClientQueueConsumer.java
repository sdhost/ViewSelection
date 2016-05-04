package JMSRole; /**
 * Created by kaijichen on 9/16/14.
 */

import javax.jms.*;
import java.io.Serializable;

public class JMSClientQueueConsumer implements Runnable, ExceptionListener, Serializable {
    private String ID = null;
    private TextMessage receivedMessage = null;
    private String ActiveMQURL = null;
    private String QueueName = null;
    private boolean isRunning = false;
    private int returnCode = -1;
    private JMSClientProvider provider = null;

    public JMSClientQueueConsumer(String URL, String QueueName, String ID){
        this.ActiveMQURL = URL;
        this.QueueName = QueueName;
        this.ID = ID;
        try {
            this.provider = new JMSClientProvider(URL, this, this.QueueName, this.ID, true, false, Session.AUTO_ACKNOWLEDGE);
        }catch (Exception e){
            System.out.println("Caught: " + e);
            this.returnCode = 2;
            e.printStackTrace();
        }
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

    public TextMessage getMessage(){
        if(this.receivedMessage != null)
            return this.receivedMessage;
        else
            return null;
    }
    public JMSClientProvider getProvider(){
        return this.provider;
    }
    public void run() {
        this.isRunning = true;
        try {
            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = this.provider.getConsumer();

            // Wait for message
            Message message = consumer.receive();

            if (message instanceof TextMessage) {
                this.receivedMessage = (TextMessage) message;
                String text = this.receivedMessage.getText();
                System.out.println("Received: " + text);
            } else {
                System.out.println("Received: " + message);
            }

            consumer.close();
            provider.close();
            returnCode = 0;
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            this.returnCode = 2;
            e.printStackTrace();
        }finally{
            this.isRunning = false;
        }
    }

    public synchronized void onException(JMSException ex) {
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
    public void close(){
        try {
            this.provider.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
