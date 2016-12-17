package com.storm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FirstN;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;

public class CountryTweetsTopology {
	//Build the topology
	public static StormTopology buildTopology(IBatchSpout spout) throws IOException {
		
		// Initialize the TridentTopology 
		final TridentTopology topology = new TridentTopology();
		/*	
		 * Define the topology:
		 *	1. TwitterSpout samples tweets
		 *	2. LanguageFilter emits only tweets of some languages
		 *	3. LanguageExtractor emits the tweet's language
		 *	4. Tuples are grouped by their value, which is the language extracted 
		 *	   before
		 *	5. A count of each language emitted is created
		 *	6. Each language and its counter are emitted. 
		 */
    
	 
     
  		//TODO Build and return the topology
		
  		// Add the stream to the topology
		topology.newStream("spout", spout)
  		// Set the language filter for each tweet
		.each(new Fields("tweet"), new LanguageFilter())
  		// Set the language extractor for each tweet
		.each(new Fields("tweet"), new LanguageExtractor(), new Fields("country"))
  		// Group tweets according to "country" fields
		.groupBy(new Fields("country"))
  		// Set the operation to be done on grouped tweets (count)
		.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
  		// Get access to the new stream to perform final stuff
		.newValuesStream()
  		// For each pair "country"-"count", call the OutputTweet() class. 
		.each(new Fields("country", "count"), new OutputTweet());
    
		//Build and return the topology
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		final Config conf = new Config();
    
		// Initialize the TwitterSpout object that act as a route of the topology.
		final IBatchSpout spout = new TwitterSpout();
    
		//If no args, assume we are testing locally
		if(args.length==0) {
			//create LocalCluster and submit
			final LocalCluster local = new LocalCluster();
			try {
				local.submitTopology("hashtag-count-topology", conf, buildTopology(spout));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			//If on a real cluster, set the workers and submit
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(spout));
	    }
	}
}
