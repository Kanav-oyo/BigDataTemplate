package com.bigdata.localcluster;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
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
    private static final String TEST_TOPIC = "test_topic";
    private static final int BROKER_ID = 0;
    private static final int BROKER_PORT = 5000;
    private static final String LOCALHOST_BROKER = String.format("localhost:%d", BROKER_PORT);


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
    public void testSomething() throws InterruptedException {

        Properties props = new Properties();

        props.put("metadata.broker.list", LOCALHOST_BROKER);
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // send one message to local kafka server:
        for (int i = 0; i < 10; i++) {
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(TEST_TOPIC, null, "test-message" + i);

            producer.send(data);
            System.out.println("sent: " + data.message());
        }
        producer.close();
        kafka.stop();
    }

    public static SimpleConsumer getNewConsumer() {
        return new SimpleConsumer("localhost", BROKER_PORT, 10000, 1024000, "test_client");
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
