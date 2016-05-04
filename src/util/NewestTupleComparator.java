package util;

import java.io.Serializable;
import java.util.Comparator;

public class NewestTupleComparator implements Comparator<TupleData>, Serializable{
    @Override
    public int compare(TupleData t1, TupleData t2){
        return (Long)t1.timestamp > (Long)t2.timestamp ? 1 : (Long)t1.timestamp == (Long)t2.timestamp ? 0 : -1;
    }

}