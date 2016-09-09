package com.stormlearn.chap2;

import java.util.Map;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeographyBolt extends BaseRichBolt {
	private IPResolver resolver;
	//private IPResolver resolver = new HttpIPResolver();
	private OutputCollector collector;
	public GeographyBolt(IPResolver resolver){
		this.resolver = resolver;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String city = input.getStringByField(Chap2Fields.CITY);
		String ip = input.getStringByField(Chap2Fields.IP);
		JSONObject json = resolver.resolveIP(ip);
		String country = (String) json.get(Chap2Fields.COUNTRY_NAME);
		collector.emit(new Values(country, city));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Chap2Fields.COUNTRY, Chap2Fields.CITY));
	}

}
