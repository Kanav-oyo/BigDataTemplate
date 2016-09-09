package com.stormlearn.chap1;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class HelloWorldBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4726326222973805939L;
	public static Logger LOG = Logger.getLogger(HelloWorldBolt.class);
	private int myCount = 0;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple input) {
		String test = input.getStringByField("sentence");
		if(test == "Hello World"){
			myCount++;
			System.out.println("Found a Hello World! My Count is now: " + Integer.toString(myCount));
			LOG.debug("Found a Hello World! My Count is now: " + Integer.toString(myCount));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("myCount"));
	}

}
