package com.stormlearn.chap2;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

public class ClickSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(ClickSpout.class);
    private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;

	    
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
        this.collector = collector;
        connectToRedis();
	}
    private void connectToRedis() {
        jedis = new Jedis(host, port);
    }
	public void nextTuple() {
		String content = jedis.rpop("count");
		if (content == null || "nil".equals(content)){
			try { Thread.sleep(300); } catch (InterruptedException e) {}
		} else {
			JSONObject obj = (JSONObject) JSONValue.parse(content);
			String ip = obj.get(Chap2Fields.IP).toString();
			String url = obj.get(Chap2Fields.URL).toString();
			String clientKey = obj.get(Chap2Fields.CLIENT_KEY).toString();
			collector.emit(new Values(ip,url,clientKey));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(Chap2Fields.IP,
												Chap2Fields.URL,
												Chap2Fields.CLIENT_KEY));
	}

}
