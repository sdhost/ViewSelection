package Runtime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import jline.internal.Log;
import ViewSelection.FeedInfo;
import ViewSelection.UserInfo;

public class Dataset {
	
	 public HashSet<FeedInfo> feeds;
	 public HashSet<UserInfo> users;
	 
	 public void storeDataset(String filename) throws IOException{
	    	File f = new File(filename);
	    	File df = new File(filename+".dis");
	    	while(f.exists()){
	    		filename += "c";
	    		f = new File(filename);
	    	}
	    	f.createNewFile();
//	    	df.deleteOnExit();
	    	df.createNewFile();
	    	FileOutputStream dfs = new FileOutputStream(df);
	    	PrintStream dps = new PrintStream(dfs);
	    	FileOutputStream fs = new FileOutputStream(f);
	    	PrintStream ps = new PrintStream(fs);
	    	ps.println(this.feeds.size() + "\t" + this.users.size());
	    	for(FeedInfo fi:feeds)
	    		ps.println(fi.toStorage());
	    	for(UserInfo ui:users){
	    		ps.println(ui.toStorage());
	    		dps.println(ui.getFollow().size());
	    	}
	    	ps.close();
	    	dps.close();
	    }
		
		public Dataset(String filename) throws IOException{
			this.feeds = new HashSet<FeedInfo>();
			this.users = new HashSet<UserInfo>();
			File f = new File(filename);
			if(!f.exists()){
				Log.error("Error reading file: " + filename);
				return;
			}
			FileInputStream fs = new FileInputStream(f);
			BufferedReader read = new BufferedReader(new InputStreamReader(fs));
			String line = read.readLine();
			int fcount = Integer.valueOf(line.split("\t")[0]);
			int ucount = Integer.valueOf(line.split("\t")[1]);
			int count = 0;
			while((line = read.readLine()) != null){
				count++;
				if(count <= fcount)
					this.feeds.add(new FeedInfo(line));
				else
					this.users.add(new UserInfo(line));
				if(count % 5000 == 0)
					System.out.println("Loaded " + count);
			}
			read.close();
			fs.close();
			if(this.feeds.size() != fcount || this.users.size() != ucount)
				Log.error("Wrong number of info readed, feed:" + this.feeds.size() + "/" + fcount + ",user:" + this.users.size() + "/"+ucount);
		}
		
		public Dataset(){
			this.feeds = new HashSet<FeedInfo>();
			this.users = new HashSet<UserInfo>();
		}
		
		public void loadEdges(String filename) throws IOException{
			File f = new File(filename);
			HashMap<Integer,FeedInfo> fmap = new HashMap<Integer,FeedInfo>();
			if(!f.exists()){
				Log.error("Error reading file: " + filename);
				return;
			}
			FileInputStream fs = new FileInputStream(f);
			BufferedReader read = new BufferedReader(new InputStreamReader(fs));
			String luser = "-1";
			UserInfo ui = null;
			String line;
			
			//For Debug Info
			int follow = 0;
			int min = Integer.MAX_VALUE;
			int max = 0;
			int count = 0;
//			BigInteger maxint = new BigInteger(String.valueOf(Integer.MAX_VALUE));
			while((line = read.readLine()) != null){
				String[] elem = line.split("\t");
				if(elem.length == 1)
					elem = line.split(" ");
				if(!elem[0].equals(luser)){
					if(ui != null){
						this.users.add(ui);
						int size = ui.getFollow().size();
						follow += ui.getFollow().size();
						if(size > max)
							max = size;
						if(size < min && size != 1)
							min = size;
						
					}
					ui = null;
//					BigInteger rawu = new BigInteger(elem[0]);
//					BigInteger sint = rawu.mod(maxint);
//					int user = sint.intValue();
//					int user = Integer.valueOf(elem[0]);
					ui = new UserInfo(Integer.valueOf(elem[0]),new HashSet<Integer>());
					luser = elem[0];
					count++;
					if(count % 100000 == 0)
						System.out.println(count);
				}
//				BigInteger rawf = new BigInteger(elem[1]);
//				BigInteger sint = rawf.mod(maxint);
//				int feed = sint.intValue();
				int feed = Integer.valueOf(elem[1]);
				ui.addFeed(feed);
				if(!fmap.containsKey(feed)){
					FeedInfo fi = new FeedInfo("NullURL",feed,"F"+feed);
					fmap.put(feed, fi);
					this.feeds.add(fi);
				}
			}
			if(ui != null && !ui.getFollow().isEmpty()){
				this.users.add(ui);
			}
			read.close();
			fs.close();
			//fmap.clear();
			
			//Remove user witch only following 1 feed
			ArrayList<UserInfo> removeu = new ArrayList<UserInfo>();
			for(UserInfo user:this.users){
				if(user.getFollow().size() <= 1){
					removeu.add(user);
					follow -= user.getFollow().size();
				}else
					for(Integer feed:user.getFollow()){
						fmap.put(feed, null);
					}
			}
			this.users.removeAll(removeu);
			ArrayList<FeedInfo> removef = new ArrayList<FeedInfo>();
			HashSet<Integer> removefid = new HashSet<Integer>();
			for(FeedInfo fi:this.feeds){
				if(!fmap.containsKey(fi.getId())){
					removefid.add(fi.getId());
				}
				
			}
			
			for(UserInfo user:this.users){
				for(Integer fi:user.getFollow()){
					if(!fmap.containsKey(fi)){
						removefid.remove(fi);
					}
				}
			}
			for(FeedInfo fi:this.feeds){
				if(removefid.contains(fi.getId()))
					removef.add(fi);
			}
			this.feeds.removeAll(removef);
			
			
			System.out.println(this.users.size() + " users and " + fmap.size() + " feeds loaded");
			System.out.println("Average follower: " + Double.valueOf((double)follow / (double)this.users.size()));
			System.out.println("Maximum follower: " + max + ", Minimum follower: " + min);
		}
		
		public void storeStat(String name) throws IOException{
			File fd = new File(name+".feed");
			File ur = new File(name+".user");
			
			fd.createNewFile();
			ur.createNewFile();
	    	FileOutputStream fds = new FileOutputStream(fd);
	    	PrintStream fps = new PrintStream(fds);
	    	FileOutputStream urs = new FileOutputStream(ur);
	    	PrintStream ups = new PrintStream(urs);
	    	
	    	HashMap<Integer,Integer> fsize = new HashMap<Integer,Integer>();
	    	for(FeedInfo feed:this.feeds){
	    		fsize.put(feed.getId(), 0);
	    	}
	    	for(UserInfo user:this.users){
	    		for(Integer f:user.getFollow()){
	    			fsize.put(f, fsize.get(f) + 1);
	    		}
	    		ups.println(user.getFollow().size());
	    		
	    	}
	    	
	    	for(Map.Entry<Integer,Integer> e:fsize.entrySet()){
	    		fps.println(e.getValue());
	    	}
	    	urs.close();
	    	fds.close();
			
		}
	 
}
