package com.psl.SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator {
	
	public FullKeyComparator() {
		super(CompositeKey.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		CompositeKey c1 = (CompositeKey) a;
		CompositeKey c2 = (CompositeKey) b;
		
		int stateCmp = c1.state.toLowerCase().compareTo(c2.state.toLowerCase());
		if(stateCmp != 0 )
			return stateCmp;
		else {
			int cityCmp = c1.city.toLowerCase().compareTo(c2.city.toLowerCase());
			if(cityCmp !=0)
				return cityCmp;
			else
				return Float.compare(c1.total, c2.total);
		}
	}
}
