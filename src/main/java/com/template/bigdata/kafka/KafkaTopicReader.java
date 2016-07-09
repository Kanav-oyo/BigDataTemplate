package com.template.bigdata.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaTopicReader {
    private final ConsumerConnector consumer;
    private final String topic;

    public KafkaTopicReader(String zookeeper, String groupId, String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
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

    public void testConsumer() {
        // TopicFilter topicfilter;
        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        // Define single thread for topic
        topicMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);

        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
        int count = 0;
        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
            while (consumerIte.hasNext()) {
                System.out.println("Message from Single Topic :: " + new String(consumerIte.next().message()));
                count++;
                // stop after 3 messages read
                if (count > 2)
                    break;
            }
        }
        if (consumer != null)
            consumer.shutdown();
    }

    public static void main(String[] args) {

        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        KafkaTopicReader simpleHLConsumer = new KafkaTopicReader(zooKeeper, groupId, topic);
        simpleHLConsumer.testConsumer();
    }

}
