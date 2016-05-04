package ViewSelection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import jline.internal.Log;
import JMSRole.JMSClientQueueProducer;

/**
 * Created by kaijichen on 10/27/14.
 */
public class UserInfo {
    private int id;
    private int alias = -1;
    private HashSet<Integer> follow;
    private HashSet<String> Query;
    private HashMap<String, String> pullQueues;
    private boolean isPush = true;
    private JMSClientQueueProducer notifier;//Pull notifier
    private String pullURL;
    private String pullQueue;
    private long lastRequest;
    private int requestIntv = 1;
    private double pullint;
    private int pullreference;

    public int getAlias(){
    	return this.alias;
    }
    
    public boolean hasAlias(){
    	if(this.alias == -1)
    		return false;
    	else
    		return true;
    }
    
    public void setAlias(int alias){
    	this.alias = alias;
    }
    
    public int getRequestIntv() {
		return requestIntv;
	}

	public void setRequestIntv(int requestIntv) {
		this.requestIntv = requestIntv;
	}

	private String URL;
    private String viewName;
    private boolean isQueue;

    public long getLastRequest() {
        return lastRequest;
    }

    public void setLastRequest(long lastRequest) {
        this.lastRequest = lastRequest;
    }

    public UserInfo(int id, HashSet<Integer> follow){
        this.id = id;
        this.follow = follow;
        this.isPush = true;

    }
    
    public void addFollow(int feed){
    	this.follow.add(feed);
    }
    
    public void setQuery(HashSet<String> views){
    	this.Query = views;
    }
    
    public HashSet<String> getQuery(){
    	return this.Query;
    }
    
    public boolean isPush(){
    	return this.isPush;
    }

    public JMSClientQueueProducer setPull(String URL, String queueName){
        this.pullURL = URL;
        this.pullQueue = queueName;
        this.isPush = false;
//        this.notifier = new JMSClientQueueProducer(pullURL,pullQueue,String.valueOf(id));
        return this.notifier;
    }
    
    public void setPull(HashMap<String,String> views){
    	this.pullQueues = views;
    	this.isPush = false;
    }

    public JMSClientQueueProducer getNotifier(){
        return this.notifier;
    }

    public void setDest(String URL, String name, boolean isQueue){
        this.URL = URL;
        this.viewName = name;
        this.isQueue = isQueue;
    }

    public String getViewName(){return this.viewName;}

    public boolean getType(){return this.isQueue;}

    public void changePush(boolean isPush){
        this.isPush = isPush;
    }

    public void addFeed(int newFeed){
        this.follow.add(newFeed);
    }

    public void removeFeed(int feed){
        this.follow.remove(feed);
    }

    public void setPush(boolean isPush) {
        this.isPush = isPush;
    }

    public HashSet<Integer> getFollow() {

        return follow;
    }
    
    public void removeFollow(HashSet<Integer> rem){
    	this.follow.removeAll(rem);
    }

    public int getId() {

        return id;
    }

    public String getURL(){
        return this.URL;
    }

	public double getpullInt() {
		return this.pullint;
	}
	
	public void setpullInt(double pullint){
		this.pullint = pullint;
	}

	public int getPullreference() {
		return pullreference;
	}

	public void setPullreference(int pullreference) {
		this.pullreference = pullreference;
	}
	
	public String toStorage(){
		String line =  String.valueOf(this.id) + "\t" +
					   String.valueOf(this.pullreference) + "\t" +
					   String.valueOf(this.pullint) + "\t" +
					   String.valueOf(this.requestIntv) + "\t";
		for(Integer f:this.follow)
			line += String.valueOf(f) + "\t";
		return line;
	}
	
	public UserInfo(String line){
		String[] elem = line.split("\t");
		if(elem.length <= 4){
			Log.error("Wrong format to initialize a UserInfo");
			return;
		}
		this.id = Integer.valueOf(elem[0]);
		this.pullreference = Integer.valueOf(elem[1]);
		this.pullint = Double.valueOf(elem[2]);
		this.requestIntv = Integer.valueOf(elem[3]);
		this.follow = new HashSet<Integer>();
		for(int i = 4;i<elem.length;i++)
			this.follow.add(Integer.valueOf(elem[i]));
	}
}
