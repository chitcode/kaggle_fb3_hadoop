package org.kaggle.fb3.featureExtract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

public class TFIDFWritable implements Writable{

	private Text content;
	private VIntWritable noOfTicket;
	private VIntWritable keyContains;
	
	TFIDFWritable(){
		set(new Text(),new VIntWritable(),new VIntWritable());
	}
	
	public void set(Text content, VIntWritable noOfTicket, VIntWritable keyContains){
		this.content = content;
		this.noOfTicket = noOfTicket;
		this.keyContains = keyContains;
	}
	
	public void set(String content, int noOfTicket, int keyContains){
		this.content = new Text(content);
		this.noOfTicket = new VIntWritable(noOfTicket);
		this.keyContains = new VIntWritable(keyContains);
	}
	
	public Text getContent() {
		return content;
	}

	public void setContent(Text content) {
		this.content = content;
	}

	public VIntWritable getNoOfTicket() {
		return noOfTicket;
	}

	public void setNoOfTicket(VIntWritable noOfTicket) {
		this.noOfTicket = noOfTicket;
	}

	public VIntWritable getKeyContains() {
		return keyContains;
	}

	public void setKeyContains(VIntWritable keyContains) {
		this.keyContains = keyContains;
	}

	@Override
	public String toString(){
		return content+"\t"+noOfTicket+"\t"+keyContains;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		content.readFields(in);
		noOfTicket.readFields(in);
		keyContains.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		content.write(out);
		noOfTicket.write(out);
		keyContains.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((content == null) ? 0 : content.hashCode());
		result = prime * result
				+ ((keyContains == null) ? 0 : keyContains.hashCode());
		result = prime * result
				+ ((noOfTicket == null) ? 0 : noOfTicket.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TFIDFWritable other = (TFIDFWritable) obj;
		if (content == null) {
			if (other.content != null)
				return false;
		} else if (!content.equals(other.content))
			return false;
		if (keyContains == null) {
			if (other.keyContains != null)
				return false;
		} else if (!keyContains.equals(other.keyContains))
			return false;
		if (noOfTicket == null) {
			if (other.noOfTicket != null)
				return false;
		} else if (!noOfTicket.equals(other.noOfTicket))
			return false;
		return true;
	}

	
	

}
