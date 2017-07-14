package com.bigdata.localcluster;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.collection.*;
import scala.collection.Iterable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Created by arunmohzi on 7/13/17.
 */
public class TestKafkaLocalServer {

    private static KafkaLocalServer kafkaLocalServer;
    private static final String DEFAULT_KAFKA_LOG_DIR = "/tmp/test/kafka_embedded";
    private static final String TEST_TOPIC = "test_topic";
    private static final int BROKER_ID = 0;
    private static final int BROKER_PORT = 5000;
    private static final String LOCALHOST_BROKER = String.format("localhost:%d", BROKER_PORT);

    private static final String DEFAULT_ZOOKEEPER_LOG_DIR = "/tmp/test/zookeeper";
    private static final int ZOOKEEPER_PORT = 2000;
    private static final String ZOOKEEPER_HOST = String.format("localhost:%d", ZOOKEEPER_PORT);

    private static final String groupId = "groupID";

    private Charset charset = Charset.forName("UTF-8");
    private CharsetDecoder decoder = charset.newDecoder();

    @BeforeClass
    public static void startKafka(){
        Properties kafkaProperties;
        Properties zkProperties;

        try {
            //load properties
            kafkaProperties = getKafkaProperties(DEFAULT_KAFKA_LOG_DIR, BROKER_PORT, BROKER_ID);
            zkProperties = getZookeeperProperties(ZOOKEEPER_PORT,DEFAULT_ZOOKEEPER_LOG_DIR);

            //start kafkaLocalServer
            kafkaLocalServer = new KafkaLocalServer(kafkaProperties, zkProperties);
            Thread.sleep(5000);
        } catch (Exception e){
            e.printStackTrace(System.out);
            fail("Error running local Kafka broker");
            e.printStackTrace(System.out);
        }

        //do other things
    }

    @Test
    public void testKafkaProducerAndConsumer() throws InterruptedException {
        List<String> expectedMessageList = new ArrayList<>();
        //produce message here
        Properties props = new Properties();
        props.put("metadata.broker.list", LOCALHOST_BROKER);
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // send one message to local kafkaLocalServer server:
        for (int i = 0; i < 10; i++) {
            String message = "test-message" + i;
            System.out.println("Producing message - " + message);
            expectedMessageList.add(message);
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(TEST_TOPIC, message);
            producer.send(data);
        }
        producer.close();

        List<String> actualMessageList = new ArrayList<>();

        SimpleConsumer kafkaconsumer = getNewConsumer(BROKER_PORT, "test_client");
        long offset = 0l;

        while (offset < 1) { // to avoid endless loop
            FetchRequest request = new FetchRequestBuilder().clientId("clientName").addFetch(TEST_TOPIC, 0, offset, 1000000).build();
            kafka.javaapi.FetchResponse fetchResponse = kafkaconsumer.fetch(request);

            for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(TEST_TOPIC, 0)) {
                offset = messageAndOffset.offset();
                String message = byteBufferToString(messageAndOffset.message().payload()).toString();
                System.out.println("Consuming message - " + message);
                actualMessageList.add(message);
            }
        }
        assertEquals(expectedMessageList.size(), actualMessageList.size());
        assertEquals(actualMessageList, expectedMessageList);
        kafkaconsumer.close();
        kafkaLocalServer.stop();
    }

    public static SimpleConsumer getNewConsumer(int port, String clientId) {
        return new SimpleConsumer("localhost", port, 10000, 1024000, clientId);
    }

    private static Properties getKafkaProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", ZOOKEEPER_HOST);
        properties.put("default.replication.factor", "1");
        properties.put("delete.topic.enable", "true");
        return properties;
    }

    private static Properties getZookeeperProperties(int port, String zookeeperDir) {
        Properties properties = new Properties();
        properties.put("clientPort", port + "");
        properties.put("dataDir", zookeeperDir);
        return properties;
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "1200000");
        props.put("zookeeper.connection.timeout.ms", "1200000");
        props.put("rebalance.backoff.ms", "60000");
        props.put("rebalance.retries.max", "20");
        props.put("auto.commit.enable", "true");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    public String byteBufferToString(ByteBuffer buffer)
    {
        String data = "";
        try {
            int old_position = buffer.position();
            data = decoder.decode(buffer).toString();
            // reset buffer's position to its original so it is not altered:
            buffer.position(old_position);
        }
        catch (Exception e) {
            return data;
        }
        return data;
    }
}
