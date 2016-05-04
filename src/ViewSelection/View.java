package ViewSelection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Created by kaijichen on 10/30/14.
 */
public class View implements Comparable<View>{
    public HashSet<Integer> feeds;
    public HashSet<Integer> alias = null;
    public ArrayList<Integer> sorted_feed;
    public String id;
    public boolean isFeed = false;
    public HashSet<View> links;
    public boolean isMaterialized = false;
    public boolean isProcessed = false;
    public String URL = "";
    public double updateFreq;
    public int updateIntv;
    public double queryFreq;
    public double bpc;

    public View(HashSet<Integer> feeds, String id, boolean isFeed, String URL){
        this.feeds = feeds;
        sorted_feed = new ArrayList<Integer>();
    	sorted_feed.addAll(feeds);
    	Collections.sort(sorted_feed);
        this.id = id;
        this.isFeed = isFeed;
        if(this.isFeed)
        	this.isMaterialized = true;
        this.links = new HashSet<View>();
        this.URL = URL;

    }
    //TODO: Uncomment code below for greedy view generation
    //		Comment for baseline solutions
    @Override
    public int hashCode() {
    	HashCodeBuilder hash = new HashCodeBuilder(17,31);
    	for(Integer f:sorted_feed)
    		hash.append(f);
    	return hash.toHashCode();
    }
    
    @Override
    public boolean equals(Object obj){
    	if(obj == null)
    		return false;
    	
    	if(!(obj instanceof View))
    		return false;
    	else{
    		View v = (View) obj;
    		if(v.sorted_feed.size() != this.sorted_feed.size())
    			return false;
    		EqualsBuilder eb = new EqualsBuilder();
    		eb.append(this.feeds.size(), v.feeds.size());
    		for(int i = 0;i<this.feeds.size();i++)
    			eb.append(this.sorted_feed.get(i), v.sorted_feed.get(i));
    		return eb.isEquals();
    	}
    }
	@Override
	public int compareTo(View o) {
		if(o.bpc > this.bpc)
			return 1;
		else if(o.bpc == this.bpc)
			return 0;
		else
			return -1;
	}
    
    public void setAlias(int uid){
    	if(this.alias == null)
    		this.alias = new HashSet<Integer>();
    	this.alias.add(uid);
    }
    
    public boolean isAlias(int uid){
    	if(this.alias == null)
    		return false;
    	else
    		return this.alias.contains(uid);
    }
    
}
