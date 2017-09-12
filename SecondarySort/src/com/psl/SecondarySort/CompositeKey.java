package com.psl.SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{
	
	public String state;
	public String city;
	public Float total;
	
	public CompositeKey() {
		// TODO Auto-generated constructor stub
	}
	
	public CompositeKey(String state , String city , Float total) {
		super();
		this.state = (state == null)?"":state;
		this.city = (city == null)?"":city;
		this.total = total;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		state = in.readUTF();
		city = in.readUTF();
		total = in.readFloat();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(state);
		out.writeUTF(city);
		out.writeFloat(total);
		
	}

	@Override
	public int compareTo(CompositeKey o) {
		
		int stateCmp = state.toLowerCase().compareTo(o.state.toLowerCase());
		if(stateCmp != 0 )
			return stateCmp;
		else {
			int cityCmp = city.toLowerCase().compareTo(o.city.toLowerCase());
			if(cityCmp !=0)
				return cityCmp;
			else
				return Float.compare(total, o.total);
		}

	}

}
