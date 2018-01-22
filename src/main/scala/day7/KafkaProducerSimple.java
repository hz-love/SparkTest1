package day7;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 *  实现一个的kafka Producer    -2017年5月23日20:02:49
 *  主要实现两个功能:
 *  1 数据发送
 *  2 数据按照自定义的Partition策略进行发送
 */
public class KafkaProducerSimple {
    public static void main(String[] args) {
        //1.指定当前kafka producer生产的数据的目的地
        String topic = "test";
        //2.配置文件信息
        Properties props=new Properties();
        //数据序列化的编码
        props.put("serializer.class","kafka.serializer.StringEncoder");
        //请求kafka的列表
        props.put("metadata.broker.list","xiaoZH:9092,Node1:9092");
        //设置发送数据是否需要服务端的反馈,有三个值0,1,-1
        //0:producer 不会等待broker发送ack
        //1:当leader接收到消息之后发送ack
        //-1:当所有的follower都同步消息成功后发送ack
        props.put("request.required.acks","1");
        //调用分区器
        props.put("partitioner.class","day7.MyLogPartitioner");

        //3.通过配置信息,创建生产者
        Producer<String, String> producer = new Producer<>(new ProducerConfig(props));

        //4.生产数据
        for (int i=1;i<1000;i++){
            String messageStr = new String(i + "Producer发送的数据");
            //5.调用Producer的send方法发送的数据
            producer.send(new KeyedMessage<String,String>(topic,messageStr));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

