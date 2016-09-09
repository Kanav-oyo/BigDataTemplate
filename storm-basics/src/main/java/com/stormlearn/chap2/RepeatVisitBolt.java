package com.stormlearn.chap2;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

public class RepeatVisitBolt extends BaseRichBolt {

    OutputCollector collector;
    Jedis jedis;

    private String host;
    private int port;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		host = stormConf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(stormConf.get(Conf.REDIS_PORT_KEY).toString());
        this.collector = collector;
        connectToRedis();
	}

    private void connectToRedis() {
       jedis = new Jedis(host, port);
    }

	@Override
	public void execute(Tuple input) {
		String ip = input.getStringByField(Chap2Fields.IP);
		String url = input.getStringByField(Chap2Fields.URL);
		String clientkey = input.getStringByField(Chap2Fields.CLIENT_KEY);
		String key = url + ":" + clientkey;
		String value = jedis.get(key);
		if(value == null){
			jedis.set(key, "visited");
			collector.emit(new Values(clientkey, url, Boolean.TRUE.toString()));
		}else {
			collector.emit(new Values(clientkey, url, Boolean.FALSE.toString()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Chap2Fields.CLIENT_KEY,
				Chap2Fields.URL,
				Chap2Fields.UNIQUE));
	}

}
