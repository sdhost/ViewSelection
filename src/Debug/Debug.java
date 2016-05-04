package Debug;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import DynamoDB.CreateTable;
import JMSRole.JMSClientQueueProducer;
import JMSRole.JMSClientTopicConsumer;
import Runtime.Dataset;
import ViewSelection.Simulator;

import javax.jms.TextMessage;

import org.apache.commons.math3.distribution.ZipfDistribution;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import util.NewestTupleComparator;
import util.TupleData;

/**
 * Created by kaijichen on 9/25/14.
 */
public class Debug {

	//Read result from ActiveMQ
//    public static void main(String[] args) throws Exception {
//        //String topicName = "OutputLatency";
//    	
//        String topicName = "OutputThroughput";
//
//        JMSClientTopicConsumer topicConsumer = new JMSClientTopicConsumer(Simulator.mqURL, topicName, "TopicConsumer");
//
//        long begin = System.currentTimeMillis();
//        while(System.currentTimeMillis() - begin < 6000000){
//            long start = System.currentTimeMillis();
//            //TextMessage msg = topicConsumer.receiveDurable();
//            TextMessage msg = topicConsumer.receive();
//            if(msg != null){
//                long now = System.currentTimeMillis();
//                System.out.println(msg.getText());
//            }
//            //Thread.sleep(5000);
//        }
//    }
	//Test sort
//	public static void main(String[] args) throws Exception {
//		Random rnd = new Random();
//		ArrayList<TupleData> counter = new ArrayList<TupleData>();
//       for(int i = 0; i < 10; i++){
//    	   TupleData tpd = new TupleData("T"+i,Math.abs(rnd.nextLong()) % 100,(long) i);
//    	   counter.add(tpd);
//       }
//       for(TupleData tp:counter)
//    	   System.out.println(tp.json + "\t" + tp.timestamp);
//       System.out.println("Top: " + counter.get(0).json);
//       System.out.println("AfterSort");
//       Collections.sort(counter, new NewestTupleComparator());
//       for(TupleData tp:counter)
//    	   System.out.println(tp.json + "\t" + tp.timestamp);
//       System.out.println("Top: " + counter.get(0).json);
//       TupleData tmp = new TupleData("Test",999l,999l);
//       counter.get(0).copy(tmp);
//       for(TupleData tp:counter)
//    	   System.out.println(tp.json + "\t" + tp.timestamp);
//       System.out.println("Top: " + counter.get(0).json);
//       System.out.println("AfterSort");
//       Collections.sort(counter, new NewestTupleComparator());
//       for(TupleData tp:counter)
//    	   System.out.println(tp.json + "\t" + tp.timestamp);
//       System.out.println("Top: " + counter.get(0).json);
//       
//    }
	//Test DynamoDB
	public static void main(String[] args) throws Exception {
		
		String fname = "gplus_combined.txt.compress";
    	Dataset ds = new Dataset();
    	ds.loadEdges(fname);
		
		
		
	}
}
