package com.microsoft.example;

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
    
    topology.newStream("spout", spout)
    .each(new Fields("tweet"), new LanguageFilter())
    .each(new Fields("tweet"), new LanguageExtractor(), new Fields("country"))
    .groupBy(new Fields("country"))
    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
    .newValuesStream()
    .applyAssembly(new FirstN(10, "count"))
    .each(new Fields("country", "count"), new OutputTweet());
    
    //Build and return the topology
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    final Config conf = new Config();
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
