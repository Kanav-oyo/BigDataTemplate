package com.bigdata.localcluster;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.fail;

/**
 * Created by arunmohzi on 7/13/17.
 */
public class MyTest {

    private static KafkaLocal kafka;
    private static final String DEFAULT_KAFKA_LOG_DIR = "/tmp/test/kafka_embedded";
    private static final String DEFAULT_ZOOKEEPER_LOG_DIR = "/tmp/test/zookeeper";
    private static final int BROKER_ID = 0;
    private static final int BROKER_PORT = 5000;
    @BeforeClass
    public static void startKafka(){
        Properties kafkaProperties;
        Properties zkProperties;

        try {
            //load properties
            kafkaProperties = getKafkaProperties(DEFAULT_KAFKA_LOG_DIR, BROKER_PORT, BROKER_ID);
            zkProperties = getZookeeperProperties();

            //start kafka
            kafka = new KafkaLocal(kafkaProperties, zkProperties);
            Thread.sleep(5000);
        } catch (Exception e){
            e.printStackTrace(System.out);
            fail("Error running local Kafka broker");
            e.printStackTrace(System.out);
        }

        //do other things
    }

    @Test
    public void testSomething() {
    }


    private static Properties getKafkaProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", "localhost:2000");
        properties.put("default.replication.factor", "1");
        properties.put("delete.topic.enable", "true");
        return properties;
    }

    private static Properties getZookeeperProperties() {
        Properties properties = new Properties();
        properties.put("clientPort", "2000");
        properties.put("dataDir", DEFAULT_ZOOKEEPER_LOG_DIR);
        return properties;
    }
}
