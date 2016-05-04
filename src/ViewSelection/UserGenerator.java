package ViewSelection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * Created by kaijichen on 11/3/14.
 */
public class UserGenerator {
    public static HashSet<UserInfo> genUniformUser(int count, int peruser, HashSet<FeedInfo> feeds){
        Random rnd = new Random(System.currentTimeMillis());
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        for(int i=0;i<count;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            while(follow.size() < peruser){
                follow.add(mapFeeds.get(rnd.nextInt(mapFeeds.size())).getId());
            }
            UserInfo ui = new UserInfo(i,follow);
            result.add(ui);
        }
        return result;
    }

    public static HashSet<UserInfo> genUniformUser(int count, int peruser, HashSet<FeedInfo> feeds, String pullURL){
        Random rnd = new Random(System.currentTimeMillis());
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        for(int i=0;i<count;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            while(follow.size() < peruser){
                follow.add(mapFeeds.get(rnd.nextInt(mapFeeds.size())).getId());
            }
            UserInfo ui = new UserInfo(i,follow);
            ui.setDest(pullURL,"User" + i + "View", true);
            ui.setPull(pullURL,ui.getViewName());
            result.add(ui);
        }
        return result;
    }
    
//    public static HashSet<UserInfo> genUniformUser(int count, int peruser, HashSet<FeedInfo> feeds, String pullURL, double low, double high){
//        Random rnd = new Random(System.currentTimeMillis());
//        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
//        for(FeedInfo fi:feeds){
//            mapFeeds.put(fi.getId(),fi);
//        }
//        Double minUpdate = Double.MAX_VALUE;
//        Double minPull = Double.MAX_VALUE;
//        HashSet<UserInfo> result = new HashSet<UserInfo>();
//        for(int i=0;i<count;i++){
//            HashSet<Integer> follow = new HashSet<Integer>();
//            int fsize = rnd.nextInt(peruser*2-1) + 2;
//            if(fsize < 2)
//            	System.out.println("fsize error");
//            while(follow.size() < fsize){
//                follow.add(mapFeeds.get(rnd.nextInt(mapFeeds.size())).getId());
//            }
//            
//            double updateint = Double.MAX_VALUE;
//            int refFeed = 0;
//            for(Integer fi:follow)
//            	if(mapFeeds.get(fi).getUpdateFreq() < updateint){
//            		updateint = mapFeeds.get(fi).getUpdateFreq();
//            		refFeed = fi;
//            	}
//            if(minUpdate > updateint)
//            	minUpdate = updateint;
//            
//            UserInfo ui = new UserInfo(i,follow);
//            ui.setRequestFreq(low + (high-low) * rnd.nextDouble());
//            ui.setPullreference(refFeed);
//            ui.setpullInt(updateint / ui.getRequestFreq());
//            if(ui.getRequestFreq() < minPull)
//            	minPull = ui.getRequestFreq();
//            //ui.setDest(pullURL,"User" + i + "View", true);
//            //ui.setPull(pullURL,ui.getViewName());
//            result.add(ui);
//        }
//        
//        return result;
//    }
    
    public static HashSet<UserInfo> genUniformUser(int count, int peruser, HashSet<FeedInfo> feeds, int low, int high){
        Random rnd = new Random(System.currentTimeMillis());
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        for(int i=0;i<count;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            int fsize = rnd.nextInt(peruser*2-1) + 2;
            if(fsize < 2)
            	System.out.println("fsize error");
            while(follow.size() < fsize){
                follow.add(mapFeeds.get(rnd.nextInt(mapFeeds.size())).getId());
            }
            
            double updateint = Double.MAX_VALUE;
            int refFeed = 0;
            for(Integer fi:follow)
            	if(mapFeeds.get(fi).getUpdateIntv() < updateint){
            		updateint = mapFeeds.get(fi).getUpdateIntv();
            		refFeed = fi;
            	}
            
            UserInfo ui = new UserInfo(i,follow);
            ui.setRequestIntv(low + rnd.nextInt(high - low));
            ui.setPullreference(refFeed);
            ui.setpullInt(ui.getRequestIntv() / updateint);
            //ui.setDest(pullURL,"User" + i + "View", true);
            //ui.setPull(pullURL,ui.getViewName());
            result.add(ui);
        }
        
        return result;
    }
    
    public static HashSet<UserInfo> genConstantUser(int count, int peruser, HashSet<FeedInfo> feeds, int intv){
        Random rnd = new Random(System.currentTimeMillis());
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        for(int i=0;i<count;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            int fsize = peruser;
            if(fsize < 2)
            	System.out.println("fsize error");
            while(follow.size() < fsize){
                follow.add(mapFeeds.get(rnd.nextInt(mapFeeds.size())).getId());
            }
            
            double updateint = Double.MAX_VALUE;
            int refFeed = 0;
            for(Integer fi:follow)
            	if(mapFeeds.get(fi).getUpdateIntv() < updateint){
            		updateint = mapFeeds.get(fi).getUpdateIntv();
            		refFeed = fi;
            	}
            
            UserInfo ui = new UserInfo(i,follow);
            ui.setRequestIntv(intv);
            ui.setPullreference(refFeed);
            ui.setpullInt(ui.getRequestIntv() / updateint);
            //ui.setDest(pullURL,"User" + i + "View", true);
            //ui.setPull(pullURL,ui.getViewName());
            result.add(ui);
        }
        
        return result;
    }

    public static HashSet<UserInfo> genHierachyUser(HashSet<FeedInfo> feeds, String pullURL){
        //Random rnd = new Random(System.currentTimeMillis());
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        int j = 0;
        int i = 0;
        for(; i < feeds.size() / 2; i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            follow.add(j++);
            follow.add(j++);
            UserInfo ui = new UserInfo(i,follow);
            ui.setDest(pullURL,"User" + i + "View", true);
            ui.setPull(pullURL,ui.getViewName());
            result.add(ui);
        }
        j = 0;
        for(;i < feeds.size() / 2 + feeds.size() / 4;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            UserInfo ui = new UserInfo(i,follow);
            ui.setDest(pullURL,"User" + i + "View", true);
            ui.setPull(pullURL,ui.getViewName());
            result.add(ui);
        }
        j = 0;
        for(;i < feeds.size() / 2 + feeds.size() / 4 + feeds.size() / 8; i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            follow.add(j++);
            UserInfo ui = new UserInfo(i,follow);
            ui.setDest(pullURL,"User" + i + "View", true);
            ui.setPull(pullURL,ui.getViewName());
            result.add(ui);
        }
        
        return result;
    }

    public static HashSet<UserInfo> genZipfUser(int count, int peruser_mean, int intv_mean, HashSet<FeedInfo> feeds, double update_param, double size_param){
        ZipfDistribution rnd_intv = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,update_param);
        int[] intv_sample = rnd_intv.sample(count);
        int max = 0;
    	for(int i=0;i<intv_sample.length;i++)
    		if(intv_sample[i] > max)
    			max = intv_sample[i];
        double mean_intv = rnd_intv.getNumericalMean();
        ZipfDistribution rnd_size = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,size_param);
        int [] size_sample = rnd_size.sample(count);
        double mean_sizesample = rnd_size.getNumericalMean() / Double.valueOf(peruser_mean);
        Random rnd = new Random(System.currentTimeMillis());
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        double total_pull_freq = 0;
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        for(int i=0;i<count;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            int fsize = (int) (Math.ceil(size_sample[i] / (mean_sizesample)) + 1);
            if(fsize < 2)
            	System.out.println("fsize error");
            while(fsize > feeds.size()){
            	fsize = (int) (Math.ceil(size_sample[i] / (mean_sizesample)) + 1);
            }
            while(follow.size() < fsize){
                follow.add(mapFeeds.get(Math.abs(rnd.nextInt(mapFeeds.size()))).getId());
            }
            
            double updateint = Double.MAX_VALUE;
            int refFeed = 0;
            for(Integer fi:follow)
            	if(mapFeeds.get(fi).getUpdateIntv() < updateint){
            		updateint = mapFeeds.get(fi).getUpdateIntv();
            		refFeed = fi;
            	}
            
            UserInfo ui = new UserInfo(i,follow);
            int reqintv = (int)Math.ceil((max - intv_sample[i]) / mean_intv * intv_mean) + 2;
            ui.setRequestIntv(reqintv);
            total_pull_freq += 1 / Double.valueOf(reqintv);
            ui.setPullreference(refFeed);
            ui.setpullInt(ui.getRequestIntv() / updateint);
            result.add(ui);
        }
        
        System.out.println("Total Pull: " + total_pull_freq);
        
        return result;
    }
    
    public static HashSet<UserInfo> genZipfUser_feed(int count, int peruser_mean, int intv_mean, HashSet<FeedInfo> feeds, double update_param, double size_param, double feed_param){
        ZipfDistribution rnd_intv = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,update_param);
        int[] intv_sample = rnd_intv.sample(count);
        int max = 0;
    	for(int i=0;i<intv_sample.length;i++)
    		if(intv_sample[i] > max)
    			max = intv_sample[i];
        double mean_intv = rnd_intv.getNumericalMean();
        ZipfDistribution rnd_size = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,size_param);
        int [] size_sample = rnd_size.sample(count);
        double mean_sizesample = rnd_size.getNumericalMean() / Double.valueOf(peruser_mean);
        ZipfDistribution rnd_feed = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,feed_param);
        
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        double total_pull_freq = 0;
        HashSet<UserInfo> result = new HashSet<UserInfo>();
        for(int i=0;i<count;i++){
            HashSet<Integer> follow = new HashSet<Integer>();
            int fsize = (int) (Math.ceil(size_sample[i] / (mean_sizesample)) + 1);
            if(fsize < 2)
            	System.out.println("fsize error");
            while(fsize > feeds.size()){
            	fsize = (int) (Math.ceil(size_sample[i] / (mean_sizesample)) + 1);
            }
            while(follow.size() < fsize){
                follow.add(mapFeeds.get(Math.abs(rnd_feed.sample() % feeds.size())).getId());
            }
            
            double updateint = Double.MAX_VALUE;
            int refFeed = 0;
            for(Integer fi:follow)
            	if(mapFeeds.get(fi).getUpdateIntv() < updateint){
            		updateint = mapFeeds.get(fi).getUpdateIntv();
            		refFeed = fi;
            	}
            
            UserInfo ui = new UserInfo(i,follow);
            int reqintv = (int)Math.ceil((max - intv_sample[i]) / mean_intv * intv_mean) + 2;
            ui.setRequestIntv(reqintv);
            total_pull_freq += 1 / Double.valueOf(reqintv);
            ui.setPullreference(refFeed);
            ui.setpullInt(ui.getRequestIntv() / updateint);
            result.add(ui);
        }
        
        System.out.println("Total Pull: " + total_pull_freq);
        
        return result;
    }
    
    public static HashSet<UserInfo> assignZipfUser(HashSet<UserInfo> users, HashSet<FeedInfo> feeds, int intv_mean, double update_param){
        ZipfDistribution rnd_intv = new ZipfDistribution(Simulator.ZIPF_SAMPLE_SIZE,update_param);
        int count = users.size();
        int max = 0;
//        int[] intv_sample = rnd_intv.sample(users.size());
        int[] intv_sample = new int[count];
//    	long begin = System.currentTimeMillis();
    	for(int i=0;i<count;i++){
    		intv_sample[i] = rnd_intv.sample();
    		if(intv_sample[i] > max)
    			max = intv_sample[i];
//    		if(i%100==0)
//    			System.out.println("Sample " + i + " : " + (System.currentTimeMillis() - begin) + " ms");
    	}
        double mean_intv = rnd_intv.getNumericalMean();
        HashMap<Integer,FeedInfo> mapFeeds = new HashMap<Integer, FeedInfo>();
        for(FeedInfo fi:feeds){
            mapFeeds.put(fi.getId(),fi);
        }
        double total_pull_freq = 0;
        int i = 0;
        for(UserInfo ui:users){
//            double updateint = Double.MAX_VALUE;
//            int refFeed = 0;
//            for(Integer fi:ui.getFollow())
//            	if(mapFeeds.get(fi).getUpdateIntv() < updateint){
//            		updateint = mapFeeds.get(fi).getUpdateIntv();
//            		refFeed = fi;
//            	}

            int reqintv = (int)Math.ceil((max - Double.valueOf(intv_sample[i])) / mean_intv * (double)intv_mean) + 2;
            ui.setRequestIntv(reqintv);
            total_pull_freq += 1 / Double.valueOf(reqintv);
//            ui.setPullreference(refFeed);
//            ui.setpullInt(ui.getRequestIntv() / updateint);
            i++;
//            if(i % 5000 == 0)
//           	 System.out.println(i);
        }
        
        System.out.println("Total Pull: " + total_pull_freq);
        
        return users;
    }

}
