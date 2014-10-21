package cn._23hours.example;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.tuple.Fields;
import cn._23hours.KafkaSpout;

public class MySqout extends KafkaSpout {
	private static final long serialVersionUID = 5562990657437328999L;

	public static final String FIELD_USER_NAME = "user_name";

	public MySqout(Properties props, Map<String, Integer> topicCountMap) {
		super(props, topicCountMap);
	}

	@Override
	public Fields generateFields() {
		return new Fields(FIELD_USER_NAME);
	}

	@Override
	public List<Object> generateTuple(byte[] message) {
		String userName = null;
		try {
			userName = new String(message, "utf-8");
			System.out.println("=========sqout log:".concat(userName));
		} catch (UnsupportedEncodingException e) {
			return null;
		}
		List<Object> tuple = new ArrayList<Object>();
		tuple.add(userName);
		return tuple;
	}

}
