package org.bigdatacourse.assignment3;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HashtagFinderBolt extends BaseBasicBolt{
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String tweet = input.getStringByField("message");
		StringTokenizer st = new StringTokenizer(tweet);
		
		System.out.println("---- Split by space ----");
		while (st.hasMoreElements()) {
			
			String term = (String) st.nextElement();
			if (StringUtils.startsWith(term, "#")) {
				String hashtag = term;
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields("hashtag") );
	}
}
