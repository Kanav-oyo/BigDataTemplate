package com.template.bigdata.StormWithKafkaSpout;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Properties;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.SchemeAsMultiScheme;


public class SpoutBuilder {
	public KafkaSpout buildKafkaSpout() {
		BrokerHosts hosts = new ZkHosts("localhost:2181");
		String topic = "testtopic";
		String zkRoot = "/kafka";
		String groupId = "testconsumergrp";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return kafkaSpout;
	}
}