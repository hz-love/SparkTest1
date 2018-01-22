package day7;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * Created by liangdmaster on 2017/5/23.
 * 实现一个Kafka Consumer
 */
public class KafkaConsumerSimple implements Runnable {

    public String title;
    public KafkaStream<byte[],byte[]> stream;

    public KafkaConsumerSimple(String title,KafkaStream<byte[],byte[]> stream){
        this.title=title;
        this.stream=stream;
    }
    @Override
    public void run() {
        System.out.println("开始运行"+title);
        //把获得的数据流放到迭代器里
        ConsumerIterator<byte[],byte[]> it=stream.iterator();

        while(it.hasNext()){
            MessageAndMetadata<byte[],byte[]> data = it.next();
            String topic = data.topic();
            int partition =data.partition();
            long offset = data.offset();
            String msg = new String(data.message());

            System.out.println(String.format("Consumer: [%s], Topic: [%s], offset: [%d], Partition: [%d], msg: [%s]",
                    title, topic, offset, partition, msg));
         }

        System.out.println(String.format("Consumer: [%s] exiting ..." + title));
    }
    public static void main(String[] args) {
        //配置参数
        Properties props = new Properties();
        props.put("group.id", "testGroup");
        props.put("zookeeper.connect", "xiaoZH:2181,Node1:2181,Node3:2181");//逗号后面不能有空格
        props.put("auto.offset.reset", "largest");
        props.put("auto.commit.interval.ms", "1000");

        //把配置信息封装到ConsumerConfig对象里
        ConsumerConfig config = new ConsumerConfig(props);

        String topic = "KafkaSimple";

        //只要ConsumerConnector还在的话，consumer会一直等待消息，不会自己退出（线程阻塞）
        ConsumerConnector connector = (ConsumerConnector) Consumer.createJavaConsumerConnector(config);

        //用来存储多个Topic
        HashMap<String, Integer> topicCountMap = new HashMap<>();
        //数字2代表用2个线程读取这个topic的信息
        topicCountMap.put(topic, 2);

        //获取数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = connector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams =topicStreamMap.get(topic);

        //创建一个固定大小的线程池
        ExecutorService executor = Executors.newFixedThreadPool(2);

        for (int i = 0; i < streams.size(); i++){
            executor.execute(new KafkaConsumerSimple("消费者：" + (i + 1), streams.get(i)));
        }
    }
}

