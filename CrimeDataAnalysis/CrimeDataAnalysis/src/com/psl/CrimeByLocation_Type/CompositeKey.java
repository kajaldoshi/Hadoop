package com.psl.CrimeByLocation_Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
	
	String locationDescription;
	String primaryType;
	
	public CompositeKey() {
		
	}
	
	public CompositeKey(String locationDescription , String primaryType ){
		super();
		this.locationDescription = locationDescription;
		this.primaryType = primaryType;
	}

	public void readFields(DataInput in) throws IOException {
		locationDescription = in.readUTF();
		primaryType = in.readUTF();
				
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(locationDescription);
		out.writeUTF(primaryType);
		
	}

	public int compareTo(CompositeKey o) {
	
		int locCmp = locationDescription.toLowerCase().compareTo(o.locationDescription.toLowerCase());
		if(locCmp != 0 )
			return locCmp;
		else {
			int typeCmp = primaryType.toLowerCase().compareTo(o.primaryType.toLowerCase());
			if(typeCmp !=0)
				return typeCmp;
			else 
				return 0;
		}	
	}
}
