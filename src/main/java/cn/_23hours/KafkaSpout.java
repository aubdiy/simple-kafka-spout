package cn._23hours;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public abstract class KafkaSpout implements IRichSpout {
	private static final long serialVersionUID = 3679050534053612196L;

	private final Properties props;
	private SpoutOutputCollector collector;
	private final Map<String, Integer> topicCountMap;
	private ConsumerConnector consumerConnector;
	private final AtomicInteger spoutPending;
	private int maxSpoutPending;
	private final int outputFieldsLength;

	public KafkaSpout(Properties props, Map<String, Integer> topicCountMap) {
		this.props = props;
		this.topicCountMap = topicCountMap;
		this.spoutPending = new AtomicInteger();
		this.outputFieldsLength = generateFields().size();
	}

	public abstract Fields generateFields();

	public abstract List<Object> generateTuple(byte[] message);

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		Object maxPending = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
		if (maxPending == null) {
			this.maxSpoutPending = 1000;
		} else {
			this.maxSpoutPending = Integer.parseInt(maxPending.toString());
		}
	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {
		consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
				.createMessageStreams(topicCountMap);
		for (Entry<String, List<KafkaStream<byte[], byte[]>>> entry : consumerMap.entrySet()) {
			List<KafkaStream<byte[], byte[]>> streamList = entry.getValue();
			ExecutorService executor = Executors.newFixedThreadPool(streamList.size());
			for (final KafkaStream<byte[], byte[]> stream : streamList) {
				executor.submit(new Runnable() {
					@Override
					public void run() {
						ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
						while (iterator.hasNext()) {
							if (spoutPending.get() <= 0) {
								sleep(1000);
								continue;
							}
							MessageAndMetadata<byte[], byte[]> next = iterator.next();
							byte[] message = next.message();
							List<Object> tuple = null;
							try {
								tuple = generateTuple(message);
							} catch (Exception e) {
								e.printStackTrace();
							}
							if (tuple == null || tuple.size() != outputFieldsLength) {
								continue;
							}
							collector.emit(tuple);
							spoutPending.decrementAndGet();
						}
					}
				});
			}
		}
	}

	@Override
	public void deactivate() {
		consumerConnector.shutdown();
	}

	@Override
	public void nextTuple() {
		if (spoutPending.get() < maxSpoutPending) {
			spoutPending.incrementAndGet();
		}
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(generateFields());
	}

	private void sleep(long millisecond) {
		try {
			Thread.sleep(millisecond);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
