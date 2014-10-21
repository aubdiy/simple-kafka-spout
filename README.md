simple-kafka-spout
==================
storm与kafka集成，在storm的spout中实现kafka的消费者，用于storm从kafka集群中取数据，由于关注与大吞吐量（kafka本身就是为此设计的），允许丢失少量数据，因此使用的是 [kafka的High Level Consumer API](http://kafka.apache.org/documentation.html#highlevelconsumerapi)

### 工具依赖：

    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka_2.9.2</artifactId>
      <version>0.8.1</version>
    </dependency>
    <dependency>
      <groupId>storm</groupId>
      <artifactId>storm</artifactId>
      <version>0.9.0.1</version>
    </dependency>

###使用方式：

* 此工具只是一个抽象类KafkaSpout，接收到的数据及在storm中发送的filed有用户自己实现
* 请参考[MySqout](https://github.com/aubdiy/simple-kafka-spout/blob/master/src/main/java/cn/_23hours/example/MySqout.java)
* 构造函数中包含两个参数：
  1.  Properties props：kafka消费者配置,请参考kafka官方文档
  2.  Map<String, Integer> topicCountMap：key:kafka的topic name, value:此kafka消费者线程数
* 实现public Fields generateFields() 方法，返回storm中sqout要发送的Fields
* 实现public List<Object> generateTuple(byte[] message)方法，接收从kafka中获取的数据，并生成tuple，tuple（List）的size要与Fields长度对应
* 完整示例[example](https://github.com/aubdiy/simple-kafka-spout/tree/master/src/main/java/cn/_23hours/example)运行MyLauncher.java
