package org.kaggle.fb3.featureExtract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TFIDFFeatExtractMapperTest {
	
	private MapDriver<LongWritable,Text,Text,TFIDFWritable> mapDriver;
	private ReduceDriver<Text, TFIDFWritable, Text, TFIDFWritable> reduceDriver;

	@Before
	public void setUp() throws Exception {
		TFIDFFeatExtractMapper featureExtractorMapper = new TFIDFFeatExtractMapper();
		mapDriver = MapDriver.newMapDriver(featureExtractorMapper);
		
		TFIDFFeatExtractReducer featureExtractorReducer = new TFIDFFeatExtractReducer();
		reduceDriver = reduceDriver.newReduceDriver(featureExtractorReducer);
	}

	@Test
	public void testMapper()  throws IOException{
		
		//String inputLine = "NEW_LINE_CHHAR\"1\",\"How to check if an uploaded file is an image without mime type?\",\"<p>I'd like to check if an  using PHP?</p>\",\"php image-processing file-upload upload mime-types\"";
		String inputLine = "NEW_LINE_CHHAR\"3\",\"R Error Invalid type (list) for variable\",\"<p>I am import matlab file and construct a data frame, matlab file contains two columns with and each row maintain a cell that has a matrix, I construct a dataframe to run random forest. But I am getting following error. </p><pre><code>Error in model.frame.default(formula = expert_data_frame$t_labels ~ ., : invalid type (list) for variable 'expert_data_frame$t_labels'</code></pre><p>Here is the code how I import the matlab file and construct the dataframe:</p><pre><code>all_exp_traintest &lt;- readMat(all_exp_filepath);len = length(all_exp_traintest$exp.traintest)/2; for (i in 1:len) { expert_train_df &lt;- data.frame(all_exp_traintest$exp.traintest[i]); labels = data.frame(all_exp_traintest$exp.traintest[i+302]); names(labels)[1] &lt;- \"\"t_labels\"\"; expert_train_df$t_labels &lt;- labels; expert_data_frame &lt;- data.frame(expert_train_df); rf_model = randomForest(expert_data_frame$t_labels ~., data=expert_data_frame, importance=TRUE, do.trace=100); }</code></pre><p>Structure of the Matlab input file</p><pre><code>[56x12 double] [56x1 double][62x12 double] [62x1 double][62x12 double] [62x1 double][62x12 double] [62x1 double][62x12 double] [62x1 double][74x12 double] [74x1 double]&gt; str(all_exp_traintest)List of 1 $ exp.traintest:List of 604 ..$ NA: num [1:56, 1:12] 0 0 0 0 8 1 1 0 0 0 ... ..$ NA: num [1:62, 1:12] 2 10 11 13 5 10 13 8 11 8 ... ..$ NA: num [1:62, 1:12] 0 0 1 0 0 0 0 0 1 1 ... ..$ NA: num [1:62, 1:12] 4 2 1 3 3 20 6 3 2 2 ... ..$ NA: num [1:62, 1:12] 2731 2362 2937 1229 1898 ... ..$ NA: num [1:74, 1:12] 27 33 34 38 33 35 36 35 47 46 ... ..$ NA: num [1:74, 1:12] 106 79 99 94 153 104 146 105 125 146 ... ..$ NA: num [1:74, 1:12] 3 9 3 0 1 26 0 4 0 0 ... ..$ NA: num [1:51, 1:12] 5 7 3 30 0 0 0 0 0 0 ... ..$ NA: num [1:66, 1:12] 0 0 13 0 0 3 2 2 0 2 ... ..$ NA: num [1:73, 1:12] 1 0 1 0 0 0 2 1 2 5 ... ..$ NA: num [1:73, 1:12] 23 14 20 14 24 22 32 61 84 278 ... ..$ NA: num [1:75, 1:12] 1 7 0 1 2 3 3 0 16 10 ... ..$ NA: num [1:90, 1:12] 10 7 8 15 25 12 37 31 18 48 ... ..$ NA: num [1:90, 1:12] 0 6 3 1 5 7 8 6 1 1 ... ..$ NA: num [1:90, 1:12] 0 1 1 2 0 4 9 6 3 4 ... ..$ NA: num [1:90, 1:12] 6 0 5 27 11 50 22 8 10 4 ... ..$ NA: num [1:90, 1:12] 3 9 13 12 4 0 5 0 5 0 ... ..$ NA: num [1:90, 1:12] 1 0 1 0 1 2 1 0 1 2 ... ..$ NA: num [1:90, 1:12] 3395 3400 3360 3770 3533 ... ..$ NA: num [1:84, 1:12] 0 0 0 0 5 0 0 5 4 2 ... ..$ NA: num [1:80, 1:12] 2 3 3 3 4 28 61 26 8 1 ... ..$ NA: num [1:81, 1:12] 4 28 22 9 16 43 80 21 19 18 ... ..$ NA: num [1:76, 1:12] 1 0 0 1 49 64 60 230 222 267 ... ..$ NA: num [1:76, 1:12] 4786 4491 2510 1144 2071 ... ..$ NA: num [1:76, 1:12] 80 128 254 109 114 267 152 139 368 363 ... ..$ NA: num [1:76, 1:12] 1 5 8 2 14 5 3 13 8 2 ... ..$ NA: num [1:76, 1:12] 10 3 8 79 4 4 11 30 2 0 ... ..$ NA: num [1:68, 1:12] 0 0 2 0 0 2 6 0 0 4 ... ..$ NA: num [1:68, 1:12] 1 4 5 2 2 3 3 1 3 0 ... ..$ NA: num [1:68, 1:12] 0 0 1 0 0 0 0 0 0 1 ... ..$ NA: num [1:69, 1:12] 39 45 2 0 1 4 3 0 13 0 ... ..$ NA: num [1:69, 1:12] 0 4 6 0 0 4 1 6 10 1 ... ..$ NA: num [1:69, 1:12] 0 2 5 2 2 2 0 0 3 6 ... ..$ NA: num [1:69, 1:12] 3 0 1 1 1 4 7 5 5 1 ... ..$ NA: num [1:66, 1:12] 5 0 0 0 0 0 0 1 3 5 ... ..$ NA: num [1:66, 1:12] 4 3 3 0 0 4 0 0 0 0 ... ..$ NA: num [1:65, 1:12] 0 0 1 0 0 0 5 8 4 1 ... ..$ NA: num [1:65, 1:12] 0 5 6 0 2 0 0 1 1 2 ... ..$ NA: num [1:69, 1:12] 0 16 5 1 14 0 1 0 0 16 ... ..$ NA: num [1:69, 1:12] 0 0 0 0 0 25 2 3 0 0 ... ..$ NA: num [1:64, 1:12] 2 0 0 0 0 0 0 0 0 0 ... ..$ NA: num [1:42, 1:12] 0 0 0 0 0 0 0 0 0 0 ... ..$ NA: num [1:67, 1:12] 0 2 4 10 15 4 1 43 1 7 ... ..$ NA: num [1:63, 1:12] 32 6 12 5 92 8 29 7 21 20 ... ..$ NA: num [1:63, 1:12] 2 5 12 8 10 13 6 11 10 14 ... ..$ NA: num [1:63, 1:12] 3 5 10 9 0 1 8 13 2 14 ... ..$ NA: num [1:54, 1:12] 0 0 14 0 0 0 0 0 0 1 ... ..$ NA: num [1:82, 1:12] 152 99 63 57 105 44 28 33 43 49 ... ..$ NA: num [1:81, 1:12] 0 1 0 0 0 0 0 0 0 0 ... ..$ NA: num [1:75, 1:12] 0 1 3 0 0 0 0 0 0 0 ... ..$ NA: num [1:75, 1:12] 1 0 0 2 0 1 0 0 0 0 ... ..$ NA: num [1:75, 1:12] 1 6 5 5 3 8 1 3 1 0 ... ..$ NA: num [1:72, 1:12] 0 0 0 0 1 0 1 2 0 0 ... ..$ NA: num [1:62, 1:12] 310 91 4 4 9 0 0 1 0 0 ... ..$ NA: num [1:62, 1:12] 239 374 1060 599 805 808 139 150 490 326 ... ..$ NA: num [1:49, 1:12] 9 18 10 12 19 5 13 10 2 3 ... ..$ NA: num [1:61, 1:12] 2 0 0 0 1 0 0 0 0 0 ... ..$ NA: num [1:61, 1:12] 4 10 16 15 8 14 10 23 11 5 ... ..$ NA: num [1:61, 1:12] 0 1 4 4 5 3 0 1 1 1 ... ..$ NA: num [1:65, 1:12] 165 100 177 65 148 58 188 55 59 62 ... ..$ NA: num [1:65, 1:12] 13 0 0 2 2 3 0 0 0 0 ... ..$ NA: num [1:66, 1:12] 157 58 101 92 15 21 73 80 78 75 ... ..$ NA: num [1:66, 1:12] 8 6 1 0 6 2 2 6 10 9 ... ..$ NA: num [1:87, 1:12] 1 2 5 6 8 3 3 3 2 3 ... ..$ NA: num [1:83, 1:12] 0 0 0 0 0 0 2 13 0 0 ... ..$ NA: num [1:81, 1:12] 0 0 1 0 3 5 3 0 2 7 ... ..$ NA: num [1:81, 1:12] 33 81 94 30 5 36 16 90 121 182 ... ..$ NA: num [1:81, 1:12] 10 11 16 6 0 0 0 1 0 0 ... ..$ NA: num [1:81, 1:12] 7 0 0 2 1 3 1 4 0 0 ... ..$ NA: num [1:81, 1:12] 1 0 5 0 2 3 1 0 1 1 ... ..$ NA: num [1:95, 1:12] 30 160 116 130 444 515 225 135 108 175 ... ..$ NA: num [1:95, 1:12] 12 1 0 10 3 3 0 4 0 0 ... ..$ NA: num [1:95, 1:12] 1 0 0 0 3 3 1 0 0 0 ... ..$ NA: num [1:95, 1:12] 11 42 61 23 41 56 81 6 83 82 ... ..$ NA: num [1:95, 1:12] 1 2 5 3 6 4 2 8 28 1 ... ..$ NA: num [1:95, 1:12] 283 192 377 216 207 261 394 262 262 554 ... ..$ NA: num [1:94, 1:12] 0 0 0 0 0 0 0 0 0 0 ... ..$ NA: num [1:72, 1:12] 0 0 0 0 0 0 0 0 0 0 ... ..$ NA: num [1:72, 1:12] 5 3 0 2 13 27 6 2 12 36 ... ..$ NA: num [1:72, 1:12] 0 2 2 0 1 0 1 4 2 2 ... ..$ NA: num [1:72, 1:12] 0 0 1 0 3 1 0 4 1 0 ... ..$ NA: num [1:67, 1:12] 27 7 18 1 2 0 0 0 0 0 ... ..$ NA: num [1:67, 1:12] 10 2 1 10 7 0 0 1 1 4 ... ..$ NA: num [1:67, 1:12] 14 17 9 20 13 20 18 13 10 7 ... ..$ NA: num [1:64, 1:12] 0 0 0 0 4 0 0 0 3 0 ... ..$ NA: num [1:64, 1:12] 3 0 1 0 2 7 13 14 4 2 ... ..$ NA: num [1:64, 1:12] 0 0 0 0 0 0 0 0 2 0 ... ..$ NA: num [1:72, 1:12] 59 61 55 120 49 202 325 244 377 551 ... ..$ NA: num [1:72, 1:12] 0 0 0 0 0 0 0 0 1 0 ... ..$ NA: num [1:72, 1:12] 0 3 1 0 1 0 0 0 4 0 ... ..$ NA: num [1:72, 1:12] 5 12 6 9 15 10 15 27 15 9 ... ..$ NA: num [1:72, 1:12] 7 0 3 0 0 1 1 1 1 0 ... ..$ NA: num [1:72, 1:12] 0 0 0 0 89 0 19 3 3 2 ... ..$ NA: num [1:61, 1:12] 5 3 5 3 3 29 46 140 49 24 ... ..$ NA: num [1:63, 1:12] 23 0 0 0 0 60 7 73 13 19 ... ..$ NA: num [1:95, 1:12] 7 96 28 2 9 5 8 190 166 1 ... ..$ NA: num [1:95, 1:12] 0 0 1 1 0 0 0 0 0 0 ... ..$ NA: num [1:95, 1:12] 4 0 2 6 6 11 6 5 6 9 ... .. [list output truncated] - attr(*, \"\"header\"\")=List of 3 ..$ description: chr \"\"MATLAB 5.0 MAT-file, Platform: MACI64, Created on: Sun Dec 9 17:35:24 2012 \"\" ..$ version : chr \"\"5\"\" ..$ endian : chr \"\"little\"\"</code></pre><p>After loading the matlab file into R</p><pre><code>all_exp_traintest$exp.traintest[1]$&lt;NA&gt; [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10] [,11] [,12] [1,] 0 0.0 0.00 0.000 0.5000 0.03125 0.015625 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000 [2,] 0 0.0 0.00 1.000 0.0625 0.03125 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000 [3,] 0 0.0 2.00 0.125 0.0625 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000 [4,] 0 4.0 0.25 0.125 0.0000 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0009765625 [5,] 8 0.5 0.25 0.000 0.0000 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0019531250 0.0000000000 [6,] 1 0.5 0.00 0.000 0.0000 0.00000 0.000000 0.0000000 0.00000000 0.003906250 0.0000000000 0.0004882812 [7,] 1 0.0 0.00 0.000 0.0000 0.00000 0.000000 0.0000000 0.00781250 0.000000000 0.0009765625 0.0009765625 [8,] 0 0.0 0.00 0.000 0.0000 0.00000 0.000000 0.0156250 0.00000000 0.001953125 0.0019531250 0.0000000000 [9,] 0 0.0 0.00 0.000 0.0000 0.00000 0.031250 0.0000000 0.00390625 0.003906250 0.0000000000 0.0004882812[10,] 0 0.0 0.00 0.000 0.0000 0.06250 0.000000 0.0078125 0.00781250 0.000000000 0.0009765625 0.0000000000[11,] 0 0.0 0.00 0.000 0.1250 0.00000 0.015625 0.0156250 0.00000000 0.001953125 0.0000000000 0.0000000000[12,] 0 0.0 0.00 0.250 0.0000 0.03125 0.031250 0.0000000 0.00390625 0.000000000 0.0000000000 0.0004882812[13,] 0 0.0 0.50 0.000 0.0625 0.06250 0.000000 0.0078125 0.00000000 0.000000000 0.0009765625 0.0000000000[14,] 0 1.0 0.00 0.125 0.1250 0.00000 0.015625 0.0000000 0.00000000 0.001953125 0.0000000000 0.0024414062[15,] 2 0.0 0.25 0.250 0.0000 0.03125 0.000000 0.0000000 0.00390625 0.000000000 0.0048828125 0.0014648438[16,] 0 0.5 0.50 0.000 0.0625 0.00000 0.000000 0.0078125 0.00000000 0.009765625 0.0029296875 0.0039062500[17,] 1 1.0 0.00 0.125 0.0000 0.00000 0.015625 0.0000000 0.01953125 0.005859375 0.0078125000 0.0151367188[18,] 2 0.0 0.25 0.000 0.0000 0.03125 0.000000 0.0390625 0.01171875 0.015625000 0.0302734375 0.0019531250[19,] 0 0.5 0.00 0.000 0.0625 0.00000 0.078125 0.0234375 0.03125000 0.060546875 0.0039062500 0.0029296875[20,] 1 0.0 0.00 0.125 0.0000 0.15625 0.046875 0.0625000 0.12109375 0.007812500 0.0058593750 0.0253906250[21,] 0 0.0 0.25 0.000 0.3125 0.09375 0.125000 0.2421875 0.01562500 0.011718750 0.0507812500 0.0253906250[22,] 0 0.5 0.00 0.625 0.1875 0.25000 0.484375 0.0312500 0.02343750 0.101562500 0.0507812500 0.0063476562[23,] 1 0.0 1.25 0.375 0.5000 0.96875 0.062500 0.0468750 0.20312500 0.101562500 0.0126953125 0.0009765625[24,] 0 2.5 0.75 1.000 1.9375 0.12500 0.093750 0.4062500 0.20312500 0.025390625 0.0019531250 0.0000000000[25,] 5 1.5 2.00 3.875 0.2500 0.18750 0.812500 0.4062500 0.05078125 0.003906250 0.0000000000 0.0019531250[26,] 3 4.0 7.75 0.500 0.3750 1.62500 0.812500 0.1015625 0.00781250 0.000000000 0.0039062500 0.0029296875[27,] 8 15.5 1.00 0.750 3.2500 1.62500 0.203125 0.0156250 0.00000000 0.007812500 0.0058593750 0.0009765625[28,] 31 2.0 1.50 6.500 3.2500 0.40625 0.031250 0.0000000 0.01562500 0.011718750 0.0019531250 0.0000000000[29,] 4 3.0 13.00 6.500 0.8125 0.06250 0.000000 0.0312500 0.02343750 0.003906250 0.0000000000 0.0083007812[30,] 6 26.0 13.00 1.625 0.1250 0.00000 0.062500 0.0468750 0.00781250 0.000000000 0.0166015625 0.0000000000[31,] 52 26.0 3.25 0.250 0.0000 0.12500 0.093750 0.0156250 0.00000000 0.033203125 0.0000000000 0.0048828125[32,] 52 6.5 0.50 0.000 0.2500 0.18750 0.031250 0.0000000 0.06640625 0.000000000 0.0097656250 0.0034179688[33,] 13 1.0 0.00 0.500 0.3750 0.06250 0.000000 0.1328125 0.00000000 0.019531250 0.0068359375 0.0229492188[34,] 2 0.0 1.00 0.750 0.1250 0.00000 0.265625 0.0000000 0.03906250 0.013671875 0.0458984375 0.0297851562[35,] 0 2.0 1.50 0.250 0.0000 0.53125 0.000000 0.0781250 0.02734375 0.091796875 0.0595703125 0.0771484375[36,] 4 3.0 0.50 0.000 1.0625 0.00000 0.156250 0.0546875 0.18359375 0.119140625 0.1542968750 0.0004882812[37,] 6 1.0 0.00 2.125 0.0000 0.31250 0.109375 0.3671875 0.23828125 0.308593750 0.0009765625 0.0000000000[38,] 2 0.0 4.25 0.000 0.6250 0.21875 0.734375 0.4765625 0.61718750 0.001953125 0.0000000000 0.0048828125[39,] 0 8.5 0.00 1.250 0.4375 1.46875 0.953125 1.2343750 0.00390625 0.000000000 0.0097656250 0.0000000000[40,] 17 0.0 2.50 0.875 2.9375 1.90625 2.468750 0.0078125 0.00000000 0.019531250 0.0000000000 0.0000000000[41,] 0 5.0 1.75 5.875 3.8125 4.93750 0.015625 0.0000000 0.03906250 0.000000000 0.0000000000 0.0000000000[42,] 10 3.5 11.75 7.625 9.8750 0.03125 0.000000 0.0781250 0.00000000 0.000000000 0.0000000000 0.0004882812[43,] 7 23.5 15.25 19.750 0.0625 0.00000 0.156250 0.0000000 0.00000000 0.000000000 0.0009765625 0.0078125000[44,] 47 30.5 39.50 0.125 0.0000 0.31250 0.000000 0.0000000 0.00000000 0.001953125 0.0156250000 0.0000000000[45,] 61 79.0 0.25 0.000 0.6250 0.00000 0.000000 0.0000000 0.00390625 0.031250000 0.0000000000 0.0000000000[46,] 158 0.5 0.00 1.250 0.0000 0.00000 0.000000 0.0078125 0.06250000 0.000000000 0.0000000000 0.0004882812[47,] 1 0.0 2.50 0.000 0.0000 0.00000 0.015625 0.1250000 0.00000000 0.000000000 0.0009765625 0.0000000000[48,] 0 5.0 0.00 0.000 0.0000 0.03125 0.250000 0.0000000 0.00000000 0.001953125 0.0000000000 0.0000000000[49,] 10 0.0 0.00 0.000 0.0625 0.50000 0.000000 0.0000000 0.00390625 0.000000000 0.0000000000 0.0000000000[50,] 0 0.0 0.00 0.125 1.0000 0.00000 0.000000 0.0078125 0.00000000 0.000000000 0.0000000000 0.0000000000[51,] 0 0.0 0.25 2.000 0.0000 0.00000 0.015625 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000[52,] 0 0.5 4.00 0.000 0.0000 0.03125 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000[53,] 1 8.0 0.00 0.000 0.0625 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000[54,] 16 0.0 0.00 0.125 0.0000 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000[55,] 0 0.0 0.25 0.000 0.0000 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000[56,] 0 0.5 0.00 0.000 0.0000 0.00000 0.000000 0.0000000 0.00000000 0.000000000 0.0000000000 0.0000000000</code></pre>\",\"r matlab machine-learning\"";
		
		mapDriver.withInput(new LongWritable(100),new Text(inputLine));
		TFIDFWritable tfidfWritable = new TFIDFWritable();
		tfidfWritable.set("r error invalid type list variable  spcharhere p spcharhere i import matlab file construct data frame matlab file contains two columns each row maintain cell has matrix constru r-error error-invalid invalid-type type-list list-variable  spcharhere-p p-spcharhere spcharhere-i i-import import-matlab matlab-file file-construct construct-data data-frame frame-matlab matlab-file file-contains contains-two two-columns columns-each each-row row-maintain maintain-cell cell-has has-matrix matrix-constru",1,1);
		mapDriver.withOutput(new Text("r"),tfidfWritable);
		
		//tfidfWritable.set("r error invalid type list variable  spcharhere p spcharhere i import matlab file construct data frame matlab file contains two columns each row maintain cell has matrix constru r-error error-invalid invalid-type type-list list-variable  spcharhere-p p-spcharhere spcharhere-i i-import import-matlab matlab-file file-construct construct-data data-frame frame-matlab matlab-file file-contains contains-two two-columns columns-each each-row row-maintain maintain-cell cell-has has-matrix matrix-constru",1,1);
		mapDriver.withOutput(new Text("matlab"),tfidfWritable);
		
		tfidfWritable.set("r error invalid type list variable  spcharhere p spcharhere i import matlab file construct data frame matlab file contains two columns each row maintain cell has matrix constru r-error error-invalid invalid-type type-list list-variable  spcharhere-p p-spcharhere spcharhere-i i-import import-matlab matlab-file file-construct construct-data data-frame frame-matlab matlab-file file-contains contains-two two-columns columns-each each-row row-maintain maintain-cell cell-has has-matrix matrix-constru",1,0);
		mapDriver.withOutput(new Text("machine-learning"),tfidfWritable);
		mapDriver.runTest();
		//fail("Not yet implemented");
	}
	
	@Test
	public void testReducer() throws IOException{
		List<TFIDFWritable> tfidfWritableList = new ArrayList<TFIDFWritable>(); 
		TFIDFWritable tfidfWritable = new TFIDFWritable();
		tfidfWritable.set("r error invalid type list variable  spcharhere p spcharhere i import matlab file construct data frame matlab file contains two columns each row maintain cell has matrix constru r-error error-invalid invalid-type type-list list-variable  spcharhere-p p-spcharhere spcharhere-i i-import import-matlab matlab-file file-construct construct-data data-frame frame-matlab matlab-file file-contains contains-two two-columns columns-each each-row row-maintain maintain-cell cell-has has-matrix matrix-constru",0,0);
		tfidfWritableList.add(tfidfWritable);
		
		tfidfWritable.set("r error invalid type list variable  spcharhere p spcharhere i import matlab file construct data frame matlab file contains two columns each row maintain cell has matrix constru r-error error-invalid invalid-type type-list list-variable  spcharhere-p p-spcharhere spcharhere-i i-import import-matlab matlab-file file-construct construct-data data-frame frame-matlab matlab-file file-contains contains-two two-columns columns-each each-row row-maintain maintain-cell cell-has has-matrix matrix-constru",1,1);
		reduceDriver.addInput(new Text("r"), tfidfWritableList);
		
		reduceDriver.withOutput(new Text("r"), tfidfWritable);
		
	}

}
