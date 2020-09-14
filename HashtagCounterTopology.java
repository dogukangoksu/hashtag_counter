package org.bigdatacourse.assignment3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

public class HashtagCounterTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout( "TweetGeneratorSpout" , new TweetGeneratorSpout() );
		builder.setBolt("HashtagFinderBolt", new HashtagFinderBolt(), 8).shuffleGrouping("TweetGeneratorSpout") ;
		builder.setBolt("HashtagCounterBolt", new HashtagCounterBolt(), 12).fieldsGrouping("HashtagFinderBolt", new Fields("hashtag")) ;
		
		Config config = new Config();
		config.setDebug(false);
		
		StormSubmitter.submitTopology("myTopology", config, builder.createTopology());
		
	
		/*LocalCluster cluster = new LocalCluster();
		
		try {
			cluster.submitTopology( "HashtagCounterTopology", config, builder.createTopology() );
			Thread.sleep(100000);
		} catch ( Exception e ) {
			e.printStackTrace();
		} finally {
			//cluster.shutdown();
		} */
	}
} 
