package com.template.bigdata.StormWithKafkaSpout;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;

public class TopologyMain	 {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaspoutObj = (new SpoutBuilder()).buildKafkaSpout();
        builder.setSpout("firstSpout", kafkaspoutObj, 1);
        builder.setBolt("firstBolt", new PrintDataBolt(), 1).shuffleGrouping("firstSpout");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(20);
            conf.setDebug(true);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

    }

}
