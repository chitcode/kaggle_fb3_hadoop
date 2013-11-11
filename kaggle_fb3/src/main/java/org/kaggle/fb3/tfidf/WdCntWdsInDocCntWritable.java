package org.kaggle.fb3.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;


public class WdCntWdsInDocCntWritable implements Writable{
		
		private VIntWritable wordsIntag;
		private VIntWritable TotalWordsIntag;
		private VIntWritable noOfTicket;
		private VIntWritable keyContains;
		
		
		public WdCntWdsInDocCntWritable(VIntWritable wordsIntag) {
			super();
			set(wordsIntag,new VIntWritable(0),new VIntWritable(0),new VIntWritable(0));
		}
		
		public WdCntWdsInDocCntWritable() {
			super();
			set(new VIntWritable(0),new VIntWritable(0),new VIntWritable(0),new VIntWritable(0));
		}
		
		public void set(VIntWritable wordsIntag, VIntWritable TotalWordsIntag, VIntWritable noOfTicket, VIntWritable keyContains){
			this.wordsIntag = wordsIntag;
			this.TotalWordsIntag = TotalWordsIntag;
			this.noOfTicket = noOfTicket;
			this.keyContains = keyContains;
		}
		
		public void set(int wordsIntag, int TotalWordsIntag, int noOfTicket, int keyContains){
			this.wordsIntag = new VIntWritable(wordsIntag);
			this.TotalWordsIntag = new VIntWritable(TotalWordsIntag);
			this.noOfTicket = new VIntWritable(noOfTicket);
			this.keyContains = new VIntWritable(keyContains);
		}
		
		

		public VIntWritable getWordsIntag() {
			return wordsIntag;
		}

		public void setWordsIntag(VIntWritable wordsIntag) {
			this.wordsIntag = wordsIntag;
		}

		public VIntWritable getTotalWordsIntag() {
			return TotalWordsIntag;
		}

		public void setTotalWordsIntag(VIntWritable totalWordsIntag) {
			TotalWordsIntag = totalWordsIntag;
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
		public void readFields(DataInput in) throws IOException {
			wordsIntag.readFields(in);
			TotalWordsIntag.readFields(in);
			noOfTicket.readFields(in);
			keyContains.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			wordsIntag.write(out);
			TotalWordsIntag.write(out);
			noOfTicket.write(out);
			keyContains.write(out);			
			
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
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((TotalWordsIntag == null) ? 0 : TotalWordsIntag
							.hashCode());
			result = prime * result
					+ ((keyContains == null) ? 0 : keyContains.hashCode());
			result = prime * result
					+ ((noOfTicket == null) ? 0 : noOfTicket.hashCode());
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
			if (wordsIntag == null) {
				if (other.wordsIntag != null)
					return false;
			} else if (!wordsIntag.equals(other.wordsIntag))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return wordsIntag + "\t" + TotalWordsIntag+"\t"+noOfTicket+"\t"+keyContains;
		}

	}	
