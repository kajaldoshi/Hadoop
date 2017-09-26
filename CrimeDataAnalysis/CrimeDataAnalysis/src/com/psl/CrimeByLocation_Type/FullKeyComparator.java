package com.psl.CrimeByLocation_Type;

import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator {
	public FullKeyComparator() {
		super(CompositeKey.class,true);
	}

	@Override
	public int compare(Object a, Object b) {
		CompositeKey c= (CompositeKey)a;
		CompositeKey o= (CompositeKey)b;
		
		int locCmp = c.locationDescription.toLowerCase().compareTo(o.locationDescription.toLowerCase());
		if(locCmp != 0 )
			return locCmp;
		else {
			int typeCmp = c.primaryType.toLowerCase().compareTo(o.primaryType.toLowerCase());
			if(typeCmp !=0)
				return typeCmp;
			else 
				return 0;
		}	
	}
}
