package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	// each map task handles one line within an adjacency matrix file
	// key: file offset
	// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int numUrls = context.getConfiguration().getInt("numUrls",1);
		String line = value.toString();
		StringBuffer sb = new StringBuffer();
		// instance an object that records the information for one webpage
		RankRecord rrd = new RankRecord(line);
		int sourceUrl, targetUrl;
		// double rankValueOfSrcUrl;
		if (rrd.targetUrlsList.size()<=0)
		{
			// there is no out degree for this webpage; 
			// scatter its rank value to all other urls
			double rankValuePerUrl = rrd.rankValue/(double)numUrls;

			for (int i=0;i<numUrls;i++)
			{
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				sb.append("#");
			}
		} 
		else 
		{
			/*Write your code here*/

			double rankValueOfCurrentNode = (double)rrd.rankValue/(double)rrd.targetUrlsList.size();
			// traversing through the list of the node in question and adding to the page rank of the neighbour

			for( Integer temp : rrd.targetUrlsList)
			{
				context.write(new LongWritable(temp.intValue()), new Text(String.valueOf(rankValueOfCurrentNode) ));
				sb.append("#").append(temp);
			}


		} //for

		//String [] urlList = value.toString().split("\t", 2);
		//sb = sb.append(urlList[1]);
		context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));
	} // end map

}


