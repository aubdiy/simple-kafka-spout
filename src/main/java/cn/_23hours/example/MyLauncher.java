package cn._23hours.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class MyLauncher {

	private static final String TOPOLOGY_NAME = "my_topology";
	private static final String KAFKA_TOPIC_NAME = "my_kafka_topic";
	private static final String KAFKA_GROUP_NAME = "my_kafka_group";

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// kafka消费者的 topic名及线程数配置
		Map<String, Integer> topicThreadCapacitys = new HashMap<String, Integer>();
		topicThreadCapacitys.put(KAFKA_TOPIC_NAME, 1);

		// kafka消费者配置
		Properties props = new Properties();
		// kafka消费者组配置
		props.put("group.id", KAFKA_GROUP_NAME);
		// kafka连接的zookeeper配置
		props.put("zookeeper.connect", "centos-224:2181/kafka_2.9.2-0.8.1.1");
		MySqout spout = new MySqout(props, topicThreadCapacitys);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", spout);
		MyBlot bolt = new MyBlot();
		builder.setBolt("bolt", bolt).shuffleGrouping("spout");

		// 本地模式启动
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, new Config(), builder.createTopology());
	}
}
