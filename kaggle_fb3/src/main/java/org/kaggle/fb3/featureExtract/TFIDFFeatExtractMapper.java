package org.kaggle.fb3.featureExtract;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFFeatExtractMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	String splitString = "\",\"";
	
	private Text outputkey = new Text();
	private Text outputValue = new Text();
	
	@Override
	protected void map(LongWritable key,Text value, Context context)throws IOException,InterruptedException{
		String entry = value.toString();
		
		String[] columns = entry.split(splitString);
		int splitSize = columns.length;
		
		String lineId = columns[0];
		lineId = lineId.replaceAll("^NEW_LINE_CHHAR\"","");
		try{
			//long l = Long.valueOf(lineId.trim());
			
			if(splitSize >= 4){
				
				String title = columns[1];
				title = title.toLowerCase();
				/***
				removed words [\s,,how, why, to, can, i , where, who, shall, can, could, should, do,from,a, an , is , are, were, was, am, the
				does, add, in, only , if,and, of , or,about,as,at,be,by,com, for,it,that,this,when,with, 's, .\S,
				(,)]
				
				**/
				
				
				title = title.replaceAll("\\,\\s", " "); //,
				title = title.replaceAll("\\,", " "); //,
				title = title.replaceAll("^how\\s|\\show\\s|\\show$", " "); //how
				title = title.replaceAll("^why\\s|\\swhy\\s|\\swhy$", " "); //why
				title = title.replaceAll("^to\\s|\\sto\\s|\\sto$", " "); //to
				title = title.replaceAll("^can\\s|\\scan\\s|\\scan$", " "); //can
				title = title.replaceAll("^i\\s|\\si\\s|\\si$", " ");//i
				title = title.replaceAll("^where\\s|\\swhere\\s|\\swhere$", " "); //where
				title = title.replaceAll("^who\\s|\\swho\\s|\\swho$", " "); //who
				title = title.replaceAll("^shall\\s|\\sshall\\s|\\sshall$", " "); //shall
				title = title.replaceAll("^can\\s|\\scan\\s|\\scan$", " "); //can
				title = title.replaceAll("^could\\s|\\scould\\s|\\scould$", " "); //could
				title = title.replaceAll("^should\\s|\\sshould\\s|\\sshould$", " "); //should
				title = title.replaceAll("^do\\s|\\sdo\\s|\\sdo$", " "); //do
				title = title.replaceAll("^from\\s|\\sfrom\\s|\\sfrom$", " "); //from
				title = title.replaceAll("^a\\s|\\sa\\s|\\sa$", " "); //a
				title = title.replaceAll("^an\\s|\\san\\s|\\san$", " "); //an
				title = title.replaceAll("^is\\s|\\sis\\s|\\sis$", " "); //is
				title = title.replaceAll("^are\\s|\\sare\\s|\\sare$", " "); //are
				title = title.replaceAll("^were\\s|\\swere\\s|\\swere$", " "); //were
				title = title.replaceAll("^was\\s|\\swas\\s|\\swas$", " "); //was
				title = title.replaceAll("^am\\s|\\sam\\s|\\sam$", " "); //am
				title = title.replaceAll("^the\\s|\\sthe\\s|\\sthe$", " "); //the
				title = title.replaceAll("^does\\s|\\sdoes\\s|\\does$", " "); //does
				title = title.replaceAll("^add\\s|\\sadd\\s|\\sadd$", " "); //add
				title = title.replaceAll("^in\\s|\\sin\\s|\\sin$", " "); //in
				title = title.replaceAll("^only\\s|\\sonly\\s|\\sonly$", " "); //only
				title = title.replaceAll("^if\\s|\\sif\\s|\\sif$", " "); //if
				title = title.replaceAll("^if\\s|\\sif\\s|\\sif$", " "); //if
				title = title.replaceAll("^for\\s|\\sfor\\s|\\sfor$", " "); //for
				title = title.replaceAll("^on\\s|\\son\\s|\\son$", " "); //on
				title = title.replaceAll("^of\\s|\\sof\\s|\\sof$", " "); //of
				title = title.replaceAll("^and\\s|\\sand\\s|\\sand$", " "); //and
				title = title.replaceAll("^or\\s|\\sor\\s|\\sor$", " "); //or
				title = title.replaceAll("^about\\s|\\sabout\\s|\\sabout$", " "); //about   as,at,be,by,com, for,it,that,this,when,with
				title = title.replaceAll("^as\\s|\\sas\\s|\\sas$", " "); //as
				title = title.replaceAll("^at\\s|\\sat\\s|\\sat$", " "); //at
				title = title.replaceAll("^be\\s|\\sbe\\s|\\sbe$", " "); //be
				title = title.replaceAll("^by\\s|\\sby\\s|\\sby$", " "); //by
				title = title.replaceAll("^com\\s|\\scom\\s|\\scom$", " "); //com
				title = title.replaceAll("^for\\s|\\sfor\\s|\\sfor$", " "); //for
				title = title.replaceAll("^it\\s|\\sit\\s|\\sit$", " "); //it
				title = title.replaceAll("^that\\s|\\sthat\\s|\\sthat$", " "); //that
				title = title.replaceAll("^this\\s|\\sthis\\s|\\sthis$", " "); //this
				title = title.replaceAll("^when\\s|\\swhen\\s|\\swhen$", " "); //when
				title = title.replaceAll("^with\\s|\\swith\\s|\\swith$", " "); //with
				title = title.replaceAll("\\'s\\s", ""); // 's
				title = title.replaceAll("\\'", ""); // '
				title = title.replaceAll("\\(", ""); // (
				title = title.replaceAll("\\)", ""); // )
				title = title.replaceAll("\\?", ""); // ?
				title = title.replaceAll("\\'t", "not"); // 't
				title = title.replaceAll("\\s\\.\\s|\\.$", " "); //.
				
				
				title = title.replace("\\s+", " "); //removing all the cumulative spaces
				
				StringBuffer titleStrBuff = new StringBuffer(title);
				titleStrBuff.append(" ");
				titleStrBuff.append(getBiGram(title));
				
				String tags = columns[splitSize-1]; //should be 3 ideally but some of the data not good
				tags = tags.replaceAll("\"?$","");
				
				
				
				for(String tag:tags.split("\\s")){
					outputkey.set(tag.trim());
					outputValue.set(titleStrBuff.toString());
					context.write(outputkey, outputValue);
				}			
				
			}else{
				//TODO: put some counters to count the number of records having problem
			}
		}catch(Exception e){
			//TODO: handle it properly
		}
	}
	
	private String getBiGram(String line){
		
		StringBuffer bigramLine = new StringBuffer();
		String[] words = line.split("\\s+");
		
		
		for(int i = 0; i < words.length-1; i++){
			bigramLine.append(words[i]);
			bigramLine.append("-");
			bigramLine.append(words[i+1]);
			bigramLine.append(" ");
		}	
		
		return bigramLine.toString().replace("\\s?$", "");
	}
}
