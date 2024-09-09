package com.dd.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.42.3:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message message = new Message("single", "someTag", body);
            //单向发送
            producer.sendOneway(message);
        }

        producer.shutdown();
        System.out.println("producer shutdown");
    }
}
