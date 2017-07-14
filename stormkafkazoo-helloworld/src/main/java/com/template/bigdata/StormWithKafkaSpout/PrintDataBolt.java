package com.template.bigdata.StormWithKafkaSpout;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintDataBolt extends BaseRichBolt {

    public static Logger LOG = Logger.getLogger(PrintDataBolt.class);

    private static final long serialVersionUID = -841805977046116528L;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

    @Override
    public void execute(Tuple input) {
		String value = input.getString(0);
		System.out.println(value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
     //
    }

}
