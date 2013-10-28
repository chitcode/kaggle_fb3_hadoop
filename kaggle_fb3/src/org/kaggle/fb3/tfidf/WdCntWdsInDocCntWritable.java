package org.kaggle.fb3.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class WdCntWdsInDocCntWritable implements Writable{
		
		private IntWritable wordsIntag;
		private IntWritable TotalWordsIntag;
		
		
		public WdCntWdsInDocCntWritable(IntWritable wordsIntag) {
			super();
			set(wordsIntag,new IntWritable(0));
		}
		
		public WdCntWdsInDocCntWritable() {
			super();
			set(new IntWritable(0),new IntWritable(0));
		}
		
		public void set(IntWritable wordsIntag, IntWritable TotalWordsIntag){
			this.wordsIntag = wordsIntag;
			this.TotalWordsIntag = TotalWordsIntag;
		}
		
		public void set(int wordsIntag, int TotalWordsIntag){
			this.wordsIntag = new IntWritable(wordsIntag);
			this.TotalWordsIntag = new IntWritable(TotalWordsIntag);
		}
		
		public void set(IntWritable wordsIntag){
			this.wordsIntag = wordsIntag;
		}

		public IntWritable getWordsIntag() {
			return wordsIntag;
		}

		public void setWordsIntag(IntWritable wordsIntag) {
			this.wordsIntag = wordsIntag;
		}

		public IntWritable getTotalWordsIntag() {
			return TotalWordsIntag;
		}

		public void setTotalWordsIntag(IntWritable totalWordsIntag) {
			TotalWordsIntag = totalWordsIntag;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			wordsIntag.readFields(in);
			TotalWordsIntag.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			wordsIntag.write(out);
			TotalWordsIntag.write(out);
		}
		
		
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((TotalWordsIntag == null) ? 0 : TotalWordsIntag
							.hashCode());
			result = prime * result
					+ ((wordsIntag == null) ? 0 : wordsIntag.hashCode());
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
			WdCntWdsInDocCntWritable other = (WdCntWdsInDocCntWritable) obj;
			if (TotalWordsIntag == null) {
				if (other.TotalWordsIntag != null)
					return false;
			} else if (!TotalWordsIntag.equals(other.TotalWordsIntag))
				return false;
			if (wordsIntag == null) {
				if (other.wordsIntag != null)
					return false;
			} else if (!wordsIntag.equals(other.wordsIntag))
				return false;
			return true;
		}
/*
		@Override
		public int compareTo(wdCntWdsInDocCntWritable wt) {
			int cmp = wordsIntag.compareTo(wt.getWordsIntag());
			if(cmp != 0){
				return cmp;
			}
			return TotalWordsIntag.compareTo(wt.getTotalWordsIntag());
		}*/

		@Override
		public String toString() {
			return wordsIntag + "\t" + TotalWordsIntag;
		}

	}	
