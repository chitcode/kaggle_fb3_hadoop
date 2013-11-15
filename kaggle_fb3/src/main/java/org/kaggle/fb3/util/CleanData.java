package org.kaggle.fb3.util;

import java.text.Normalizer;

public class CleanData {
	
	
	
	/***
	removed words [\s,,how, why, to, can, i , where, who, shall, can, could, should, do,from,a, an , is , are, were, was, am, the
	does, add, in, only , if,and, of , or,about,as,at,be,by,com, for,it,that,this,when,with, 's, .\S,
	(,)]
	
	**/	
	public static String clean(String inString){
		
		//normalizing the input String
		inString = Normalizer.normalize(inString, Normalizer.Form.NFD);
		inString = inString.replaceAll("[^\\p{ASCII}]", "");
		
		//lowering the case
		inString = inString.toLowerCase();
		
		inString = inString.replaceAll("\\,\\s", " "); //,
		inString = inString.replaceAll("\\,", " "); //,
		inString = inString.replaceAll("\\'s\\s", " "); // 's		
		inString = inString.replaceAll("\\(", " "); // (
		inString = inString.replaceAll("\\)", " "); // )
		inString = inString.replaceAll("\\<", " "); // <
		inString = inString.replaceAll("\\>", " "); // >
		inString = inString.replaceAll("\\[", " "); // [
		inString = inString.replaceAll("\\]", " "); // ]
		inString = inString.replaceAll("\\{", " "); // {
		inString = inString.replaceAll("\\}", " "); // }
		inString = inString.replaceAll("\\?", " "); // ?
		inString = inString.replaceAll("\\~", " "); // ~
		inString = inString.replaceAll("n\\'t", " not"); // 't
		inString = inString.replaceAll("\\snot\\s", " ");
		//inString = inString.replaceAll("\\!", "");
		inString = inString.replaceAll("\"", " ");
		inString = inString.replaceAll("\\'", " "); //
		inString = inString.replaceAll("\\.", " "); // .
		inString = inString.replaceAll("\\%", " "); // %
		inString = inString.replaceAll("\\&", " "); // &
		inString = inString.replaceAll("\\;", " "); //;
		
		//inString = inString.replaceAll("\\.+", "."); // .'
		//inString = inString.replaceAll("^\\s\\.^\\s", " dothere ");
		inString = inString.replaceAll("\\s[0-9]+\\s", " 00 ");		
		
		inString = inString.replaceAll("[\\!\\^\\-\\|\\+\\{\\}\\[\\]\\$\\#\\<\\>\\:\\_\\*\\=\\/\\\\]+", " "); //replacing all special characters to " 1 "
		inString = inString.replaceAll("\\s\\.\\s|\\.$", " "); //.		
		
		inString = inString.replaceAll("^lt\\s|\\slt\\s|\\slt$", " "); // lt
		inString = inString.replaceAll("^gt\\s|\\sgt\\s|\\sgt$", " "); // gt
		inString = inString.replaceAll("^amp\\s|\\samp\\s|\\samp$", " "); // lt
		inString = inString.replaceAll("\\.", ""); // .
		inString = inString.replaceAll("^how\\s|\\show\\s|\\show$", " "); //how
		inString = inString.replaceAll("^why\\s|\\swhy\\s|\\swhy$", " "); //why
		inString = inString.replaceAll("^to\\s|\\sto\\s|\\sto$", " "); //to
		inString = inString.replaceAll("^can\\s|\\scan\\s|\\scan$", " "); //can
		inString = inString.replaceAll("^i\\s|\\si\\s|\\si$", " ");//i
		inString = inString.replaceAll("^where\\s|\\swhere\\s|\\swhere$", " "); //where
		inString = inString.replaceAll("^who\\s|\\swho\\s|\\swho$", " "); //who
		inString = inString.replaceAll("^shall\\s|\\sshall\\s|\\sshall$", " "); //shall
		inString = inString.replaceAll("^can\\s|\\scan\\s|\\scan$", " "); //can
		inString = inString.replaceAll("^could\\s|\\scould\\s|\\scould$", " "); //could
		inString = inString.replaceAll("^should\\s|\\sshould\\s|\\sshould$", " "); //should
		inString = inString.replaceAll("^do\\s|\\sdo\\s|\\sdo$", " "); //do
		inString = inString.replaceAll("^from\\s|\\sfrom\\s|\\sfrom$", " "); //from
		inString = inString.replaceAll("^a\\s|\\sa\\s|\\sa$", " "); //a
		inString = inString.replaceAll("^an\\s|\\san\\s|\\san$", " "); //an
		inString = inString.replaceAll("^is\\s|\\sis\\s|\\sis$", " "); //is
		inString = inString.replaceAll("^are\\s|\\sare\\s|\\sare$", " "); //are
		inString = inString.replaceAll("^were\\s|\\swere\\s|\\swere$", " "); //were
		inString = inString.replaceAll("^was\\s|\\swas\\s|\\swas$", " "); //was
		inString = inString.replaceAll("^am\\s|\\sam\\s|\\sam$", " "); //am
		inString = inString.replaceAll("^the\\s|\\sthe\\s|\\sthe$", " "); //the
		inString = inString.replaceAll("^does\\s|\\sdoes\\s|\\does$", " "); //does
		inString = inString.replaceAll("^add\\s|\\sadd\\s|\\sadd$", " "); //add
		inString = inString.replaceAll("^in\\s|\\sin\\s|\\sin$", " "); //in
		inString = inString.replaceAll("^only\\s|\\sonly\\s|\\sonly$", " "); //only
		inString = inString.replaceAll("^if\\s|\\sif\\s|\\sif$", " "); //if
		inString = inString.replaceAll("^if\\s|\\sif\\s|\\sif$", " "); //if
		inString = inString.replaceAll("^for\\s|\\sfor\\s|\\sfor$", " "); //for
		inString = inString.replaceAll("^on\\s|\\son\\s|\\son$", " "); //on
		inString = inString.replaceAll("^of\\s|\\sof\\s|\\sof$", " "); //of
		inString = inString.replaceAll("^and\\s|\\sand\\s|\\sand$", " "); //and
		inString = inString.replaceAll("^or\\s|\\sor\\s|\\sor$", " "); //or
		inString = inString.replaceAll("^about\\s|\\sabout\\s|\\sabout$", " "); //about 
		inString = inString.replaceAll("^as\\s|\\sas\\s|\\sas$", " "); //as
		inString = inString.replaceAll("^at\\s|\\sat\\s|\\sat$", " "); //at
		inString = inString.replaceAll("^be\\s|\\sbe\\s|\\sbe$", " "); //be
		inString = inString.replaceAll("^by\\s|\\sby\\s|\\sby$", " "); //by
		inString = inString.replaceAll("^com\\s|\\scom\\s|\\scom$", " "); //com
		inString = inString.replaceAll("^for\\s|\\sfor\\s|\\sfor$", " "); //for
		inString = inString.replaceAll("^it\\s|\\sit\\s|\\sit$", " "); //it
		inString = inString.replaceAll("^that\\s|\\sthat\\s|\\sthat$", " "); //that
		inString = inString.replaceAll("^this\\s|\\sthis\\s|\\sthis$", " "); //this
		inString = inString.replaceAll("^when\\s|\\swhen\\s|\\swhen$", " "); //when
		inString = inString.replaceAll("^with\\s|\\swith\\s|\\swith$", " "); //with
		inString = inString.replaceAll("^my\\s|\\smy\\s", " "); //my
		inString = inString.replaceAll("\\shave\\s", " "); //have
		
		inString = inString.replaceAll("^[0-9]\\s|\\s[0-9]\\s|\\s[0-9]$", " "); //have 
		
		inString = inString.trim();
		
		inString = inString.replaceAll("\\s+", " "); //removing all the cumulative spaces
		//inString = inString.replace("{spcharhere\\s}+", "spcharhere ");
		//inString = inString.replace("{numberhere\\s}+ ", "numberhere ");
		return inString;
	}

}
