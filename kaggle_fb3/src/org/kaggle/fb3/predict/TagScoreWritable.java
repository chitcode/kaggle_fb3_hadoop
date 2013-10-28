package org.kaggle.fb3.predict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TagScoreWritable implements Writable{
	
	private Text tag;
	private FloatWritable score;
	
	TagScoreWritable(){
		set(new Text(),new FloatWritable());
	}
	
	public void set(Text tag, FloatWritable score){
		this.tag = tag;
		this.score = score;
	}
	
	public Text getTag() {
		return tag;
	}
	public void setTag(Text tag) {
		this.tag = tag;
	}
	public FloatWritable getScore() {
		return score;
	}
	public void setScore(FloatWritable score) {
		this.score = score;
	}		
	
	@Override
	public String toString(){
		return tag+"\t"+score;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		score.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		score.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((score == null) ? 0 : score.hashCode());
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
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
		TagScoreWritable other = (TagScoreWritable) obj;
		if (score == null) {
			if (other.score != null)
				return false;
		} else if (!score.equals(other.score))
			return false;
		if (tag == null) {
			if (other.tag != null)
				return false;
		} else if (!tag.equals(other.tag))
			return false;
		return true;
	}
	
	


}
