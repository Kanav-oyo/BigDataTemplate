package com.bigdata.localcluster;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by arunmohzi on 7/13/17.
 */
public class KafkaLocal {
    private static final String DEFAULT_LOG_DIR = "kafka_embedded";
    private static final String TEST_TOPIC = "topic";
    private static final int BROKER_ID = 0;
    private static final int BROKER_PORT = 9092;

    public KafkaServerStartable kafka;
    public ZooKeeperLocal zookeeper;

    public KafkaLocal(){

    }

    public KafkaLocal(Properties kafkaProperties, Properties zkProperties) throws IOException, InterruptedException{
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        //start local zookeeper
        System.out.println("starting local zookeeper...");
        zookeeper = new ZooKeeperLocal(zkProperties);
        System.out.println("done");

        //start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
        System.out.println("starting local kafka broker...");
        kafka.startup();
        System.out.println("done");
    }


    public void stop(){
        //stop kafka broker
        System.out.println("stopping kafka...");
        kafka.shutdown();
        System.out.println("done");
    }

    public static Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", "localhost:2000");
        properties.put("default.replication.factor", "1");
        properties.put("delete.topic.enable", "true");
        return properties;
    }
}