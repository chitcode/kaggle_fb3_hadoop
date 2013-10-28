package org.kaggle.fb3.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class WordTagWritable implements WritableComparable<WordTagWritable>{
	
	private Text word;
	private Text tag;
	
	
	public WordTagWritable(Text word, Text tag) {
		super();
		set(word,tag);
	}
	
	public WordTagWritable() {
		super();
		set(new Text(),new Text());
	}
	
	public WordTagWritable(String word, String tag) {
		super();
		set(word,tag);
	}
	
	public void set(Text word, Text tag){
		this.word = word;
		this.tag = tag;
	}
	public void set(String word, String tag){
		this.word = new Text(word);
		this.tag = new Text(tag);
	}


	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		tag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		tag.write(out);
	}
	
	@Override
	public String toString(){
		return tag+"\t"+word;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		WordTagWritable other = (WordTagWritable) obj;
		if (tag == null) {
			if (other.tag != null)
				return false;
		} else if (!tag.equals(other.tag))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}
	
	@Override
	public int compareTo(WordTagWritable wt) {
		int cmp = tag.compareTo(wt.getTag());
		if(cmp != 0){
			return cmp;
		}
		return word.compareTo(wt.getWord());
	}

}	
