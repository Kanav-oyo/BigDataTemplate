package com.stormlearn.chap2;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VisitStatsBolt extends BaseRichBolt {

	private OutputCollector collector;
	private int uniqueCount = 0;
	private int total = 0;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		boolean unique = Boolean.parseBoolean(input.getStringByField(Chap2Fields.UNIQUE));
		total++;
		if (unique) uniqueCount++;
		collector.emit(new Values(total,uniqueCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new backtype.storm.tuple.Fields(Chap2Fields.TOTAL_COUNT,
                Chap2Fields.TOTAL_UNIQUE));
	}

}
