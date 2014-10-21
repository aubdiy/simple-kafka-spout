package cn._23hours.example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MyBlot extends BaseBasicBolt {
	private static final long serialVersionUID = -4435832857439837947L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String userName = input.getStringByField(MySqout.FIELD_USER_NAME);
		System.out.println("=========blot log:".concat(userName));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
