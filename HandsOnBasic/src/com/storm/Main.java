package com.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Main {
    public static void main(String[] args) throws Exception{
        //Create Config instance for cluster configuration
        Config config = new Config();
        config.setDebug(false);
          
        //
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("number-reader-spout", new NumberReaderSpout());

        builder.setBolt("counter-bolt", new CounterBolt()).shuffleGrouping("number-reader-spout"); 
        builder.setBolt("summation-bolt", new SummationBolt()).shuffleGrouping("number-reader-spout");
        
        builder.setBolt("average-bolt", new AverageBolt()).shuffleGrouping("counter-bolt").shuffleGrouping("summation-bolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Main", config, builder.createTopology());
        Utils.sleep(60000);
        
        //Stop the topology
        cluster.killTopology("Main");
          
        cluster.shutdown();
     }
}
