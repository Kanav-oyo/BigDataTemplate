package com.template.bigdata.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/* 
 * Author : Arunmozhi, 
 * Date: July, 9th 2016
 * Below book is huge reference to build this template.
 * Learning Apache Kafka - Second Edition, By: Nishant Garg
 */
public class KafkaTopicProducer {
	private static Producer<String, String> producer;
	
	public void configureProducerProperties(String zookeeperhost) {
		Properties props = new Properties();
		props.put("metadata.broker.list", zookeeperhost);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	private void publishMessage(String topic, int messageCount) {
		for (int mCount = 0; mCount < messageCount; mCount++) {
			String runtime = new Date().toString();
			String msg = "Message Publishing Time - " + runtime;
			System.out.println(msg);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
			producer.send(data);
		}
		producer.close();
	}

	public static void main(String[] args) {
		int argsCount = args.length;
		if (argsCount == 0 || argsCount == 1)
			throw new IllegalArgumentException("Please provide topic name and Message count as arguments");
		String zookeeperhost = (String) args[0];
		String topic = (String) args[1];
		String count = (String) args[2];
		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - " + topic);
		System.out.println("Message Count - " + messageCount);

		KafkaTopicProducer simpleProducer = new KafkaTopicProducer();
		simpleProducer.configureProducerProperties(zookeeperhost);
		simpleProducer.publishMessage(topic, messageCount);
	}
}
