package com.dd.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class OrderedProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.42.3:9876");
        producer.start();

        for (int i = 0; i < 100; i++) {
            Integer orderId = i;
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("TopicA", "TagA", body);
            //将orderId作为消息key
            msg.setKeys(orderId.toString());
            /**
             * send（）的第三个参数值会传递给选择器的select()的第三个参数
             */
            //该send()为同步发送
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                //具体的选择算法在该方法中定义
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    //1.使用消息key作为选择key的选择算法
                    String keys = msg.getKeys();
                    Integer id = Integer.valueOf(keys);

                    //2.使用arg参数作为选择key的选择算法
                    //Integer id = (Integer) o;
                    int index = id % list.size();
                    return list.get(index);
                }
            }, orderId);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
