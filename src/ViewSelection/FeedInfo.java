package ViewSelection;

import jline.internal.Log;

/**
 * Created by kaijichen on 10/27/14.
 */
public class FeedInfo {
    public String getURL() {
        return URL;
    }

    public Integer getId() {
        return id;
    }

    public String getQueueName() {
        return queueName;
    }

    private String URL;//ActiveMQ URL
    private Integer id;
    private String queueName;
    
    private int updateIntv = 1;

    //private int messageFreq = 10;//Time in seconds between two messages generated by the feed

    public int getUpdateIntv() {
		return updateIntv;
	}

	public void setUpdateIntv(int updateIntv) {
		this.updateIntv = updateIntv;
	}

	public FeedInfo(String URL, Integer id, String queueName){
        this.URL = URL;
        this.id = id;
        this.queueName = queueName;
    }
	
	public FeedInfo(String line){
		String[] elem = line.split("\t");
		if(elem.length != 2){
			Log.error("Wrong format to initialize a FeedInfo");
			return;
		}
		this.id = Integer.valueOf(elem[0]);
//		this.URL = elem[1];
//		this.queueName = elem[2];
		this.updateIntv = Integer.valueOf(elem[1]);
	}

	public String toStorage(){
		return String.valueOf(id) + "\t" +
//			   String.valueOf(URL) + "\t" +
//			   String.valueOf(queueName) + "\t" +
			   String.valueOf(updateIntv);
		
	}

}
