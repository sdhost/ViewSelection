package ViewSelection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * Created by kaijichen on 11/3/14.
 */
public class FeedGenerator {
    public static HashSet<FeedInfo> genFeed(int count, String URL){
        HashSet<FeedInfo> result = new HashSet<FeedInfo>();
        for(int i=0;i<count;i++) {
            String queueName = "FeedQueue" + i;
            FeedInfo fi = new FeedInfo(URL, i, queueName);
            result.add(fi);
        }
        return result;
    }
    
    public static HashSet<FeedInfo> genFeed(int count, String URL, int lower, int higher){
    	Random rnd = new Random(System.currentTimeMillis());
        HashSet<FeedInfo> result = new HashSet<FeedInfo>();
        for(int i=0;i<count;i++) {
            String queueName = "FeedQueue" + i;
            FeedInfo fi = new FeedInfo(URL, i, queueName);
            fi.setUpdateIntv(lower + rnd.nextInt(higher - lower));
            result.add(fi);
        }
        return result;
    }
    
    public static HashSet<FeedInfo> genConstantFeed(int count, String URL, int intv){
//    	Random rnd = new Random(System.currentTimeMillis());
        HashSet<FeedInfo> result = new HashSet<FeedInfo>();
        for(int i=0;i<count;i++) {
            String queueName = "FeedQueue" + i;
            FeedInfo fi = new FeedInfo(URL, i, queueName);
            fi.setUpdateIntv(intv);
            result.add(fi);
        }
        return result;
    }


    public static HashSet<FeedInfo> genFeed(int count, LinkedList<String> URLs, int group){
        HashSet<FeedInfo> result = new HashSet<FeedInfo>();
        int interval = count / group;
        int id = 0;
        for(String URL:URLs)
            for(int i=0;i<interval;i++){
                String queueName = "FeedQueue" + id;
                FeedInfo fi = new FeedInfo(URL, id, queueName);
                result.add(fi);
                id++;
            }
        for(int i=id;i<count;i++) {
            String queueName = "FeedQueue" + i;
            FeedInfo fi = new FeedInfo(URLs.peekLast(), i, queueName);
            result.add(fi);
        }

        return result;
    }
    
    public static HashSet<FeedInfo> genZipfFeed(int count, String URL, int mean, double param){
    	ZipfDistribution rnd = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,param);
    	int[] samples = rnd.sample(count);
    	int max = 0;
    	for(int i=0;i<samples.length;i++)
    		if(samples[i] > max)
    			max = samples[i];
    	double num_mean = rnd.getNumericalMean();
    	HashSet<FeedInfo> result = new HashSet<FeedInfo>();
    	double total_push = 0;
    	for(int i=0;i<count;i++) {
            String queueName = "FeedQueue" + i;
            FeedInfo fi = new FeedInfo(URL, i, queueName);
            int update = (int) Math.ceil((max - samples[i])/num_mean*mean) + 2;
            total_push += 1.0 / Double.valueOf(update);
            fi.setUpdateIntv(update);
            result.add(fi);
        }
    	System.out.println("Total Push: " + total_push);
        return result;
    }
    
    public static HashSet<FeedInfo> assignZipfFeed(HashSet<FeedInfo> feeds, int mean, double param){
    	int count = feeds.size();
    	ZipfDistribution rnd = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,param);
    	int[] samples = new int[count];
//    	long begin = System.currentTimeMillis();
    	for(int i=0;i<count;i++){
    		samples[i] = rnd.sample();
//    		if(i%100==0)
//    			System.out.println("Sample " + i + " : " + (System.currentTimeMillis() - begin) + " ms");
    	}
//    	int[] samples = rnd.sample(feeds.size());
    	int max = 0;
    	for(int i=0;i<samples.length;i++)
    		if(samples[i] > max)
    			max = samples[i];
    	double num_mean = rnd.getNumericalMean();
    	double total_push = 0;
    	int i = 0;
//    	begin = System.currentTimeMillis();
    	for(FeedInfo fi:feeds){
    		String queueName = "FeedQueue" + fi.getId();
    		 int update = (int) Math.ceil((max - (double)samples[i])/num_mean*(double)mean) + 2;
             total_push += 1.0 / Double.valueOf(update);
             fi.setUpdateIntv(update);
             i++;
//             if(i % 5000 == 0)
//            	 System.out.println((System.currentTimeMillis() - begin) + ":" + String.valueOf(i));
    	}
    	System.out.println("Total Push: " + total_push);
    	return feeds;
    }

//    public static HashSet<FeedInfo> genFeed (HashMap<Integer, String> feedmap){
//        HashSet<FeedInfo> result = new HashSet<FeedInfo>();
//
//
//        return result;
//    }
}
