package util;

import java.io.Serializable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.minlog.Log;

import backtype.storm.tuple.Tuple;

public class TupleData implements Serializable{
	public String json;
	public Long timestamp;
	public Integer SID;
	public TupleData(String json, Long timestamp, Integer sid){
		this.json = json;
		this.timestamp = timestamp;
		this.SID = sid;
	}
	public TupleData(String line){
		String[] elem = line.split("\t");
		this.SID = Integer.valueOf(elem[elem.length - 1]);
		this.timestamp = Long.valueOf(elem[elem.length - 2]);
		this.json = "";
		for(int i = 0; i < elem.length - 2; i++)
			this.json += elem[i] + "\t";
		this.json = this.json.substring(0, this.json.length()-1);
	}
	public TupleData(Tuple tuple){
		if(tuple.size() != 3){
			Log.error("wrong type of tuple transformed!");
			return;
		}
		this.json = tuple.getStringByField("json");
		this.timestamp = tuple.getLongByField("timestamp");
		this.SID = tuple.getIntegerByField("SID");
	}
	
	public void copy(TupleData t){
		this.json = t.json;
		this.SID = t.SID;
		this.timestamp = t.timestamp;
	}
	public void copy(Tuple tuple) {
		this.json = tuple.getStringByField("json");
		this.timestamp = tuple.getLongByField("timestamp");
		this.SID = tuple.getIntegerByField("SID");
	}
	public void copy(String line) {
		String[] elem = line.split("\t");
		if(elem.length != 3){
			Log.error("Wrong message sent to SortByTimeBolt");
			return;
		}
		this.SID = Integer.valueOf(elem[2]);
		this.timestamp = Long.valueOf(elem[1]);
		this.json = elem[0];
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof TupleData))
            return false;
        if (obj == this)
            return true;

        TupleData rhs = (TupleData) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(json, rhs.json).
            append(timestamp, rhs.timestamp).
            append(SID, rhs.SID).
            isEquals();
	}
	
	@Override
	public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(json).
            append(timestamp).
            append(SID).
            toHashCode();
    }
}
