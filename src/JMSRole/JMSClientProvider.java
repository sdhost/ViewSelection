package JMSRole; /**
 * Created by kaijichen on 9/17/14.
 */
import backtype.storm.contrib.jms.JmsProvider;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;

public class JMSClientProvider implements JmsProvider, Serializable{
    private ActiveMQConnectionFactory connectionFactory = null;
    private transient Connection connection = null;
    private transient TopicConnection topicConnection = null;
    private transient Session session = null;
    private transient TopicSession topicSession = null;
    private Destination destination;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private TopicSubscriber topicConsumer;

    public JMSClientProvider(String ActiveMQURL, String Name, boolean type, boolean transacted, int auto_ack) throws JMSException{
        //Constructer for Producers
        this.connectionFactory = new ActiveMQConnectionFactory(ActiveMQURL);
        this.connection = this.connectionFactory.createConnection();
        this.connection.start();
        this.session = this.connection.createSession(transacted,auto_ack);
        if(type){
            destination = session.createQueue(Name);
        }else{
            destination = session.createTopic(Name);
        }

    }

    public JMSClientProvider(String ActiveMQURL, ExceptionListener listener, String Name, boolean type,boolean transacted, int auto_ack) throws JMSException{
        //Constructer for Consumers
        this.connectionFactory = new ActiveMQConnectionFactory(ActiveMQURL);
        this.connection = this.connectionFactory.createConnection();
        this.connection.setExceptionListener(listener);
        this.connection.start();
        this.session = this.connection.createSession(transacted,auto_ack);
        if(type){
            destination = session.createQueue(Name);
        }else{
            destination = session.createTopic(Name);
        }
    }

    public JMSClientProvider(String ActiveMQURL, ExceptionListener listener, String Name, String ID, boolean type,boolean transacted, int auto_ack) throws JMSException{
        //Constructer for Consumers
        this.connectionFactory = new ActiveMQConnectionFactory(ActiveMQURL);
        this.topicConnection = this.connectionFactory.createTopicConnection(ID,ID);
        this.topicConnection.setExceptionListener(listener);
        this.topicConnection.setClientID(ID);
        this.topicConnection.start();
        this.topicSession = this.topicConnection.createTopicSession(transacted, auto_ack);
        if(type){
            destination = session.createQueue(Name);
        }else{
            destination = this.topicSession.createTopic(Name);
        }
    }



    public Connection getConnection() throws JMSException{
        return this.connection;
    }


    public MessageProducer getProducer(int persisted) throws JMSException{
        producer = session.createProducer(destination);

        producer.setDeliveryMode(persisted);
        return producer;
    }

    public MessageConsumer getConsumer() throws JMSException{
        consumer = session.createConsumer(destination);
        return consumer;
    }

    public TopicSubscriber getDurableSubscriber(String name) throws JMSException{
        topicConsumer = topicSession.createDurableSubscriber((Topic)this.destination, name);
        return topicConsumer;
    }

    public void close() throws JMSException{
        if(this.connection != null && this.session != null){
            this.session.close();;
            this.connection.close();
            this.session = null;
            this.connection = null;
        }else{
            this.topicSession.close();;
            this.topicConnection.close();
            this.topicSession = null;
            this.topicConnection = null;
        }
    }

    public Session getSession(){
        return this.session;
    }

    @Override
    public ConnectionFactory connectionFactory() throws Exception {
        return this.connectionFactory;
    }

    @Override
    public Destination destination() throws Exception {
        return this.destination;
    }
}
