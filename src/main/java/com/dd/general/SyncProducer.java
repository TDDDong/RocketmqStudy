package com.dd.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class SyncProducer {
    public static void main(String[] args) throws Exception {
        //创建一个producer 参数为Producer Group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定nameServer地址
        producer.setNamesrvAddr("192.168.42.3:9876");
        //设置当发送失败时重试发送的次数 默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时时限为5s 默认3s
        producer.setSendMsgTimeout(5000);

        //开启生产者
        producer.start();

        //生产并发送100条消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("someTopic", "someTag", body);
            //为消息指定key
            msg.setKeys("key-" + i);
            //同步发送消息
            /**
             * // 消息发送的状态
             * public enum SendStatus {
             *      SEND_OK, // 发送成功
             *      FLUSH_DISK_TIMEOUT, // 刷盘超时。当Broker设置的刷盘策略为同步刷盘时才可能出
             *                             现这种异常状态。异步刷盘不会出现
             *      FLUSH_SLAVE_TIMEOUT, // Slave同步超时。当Broker集群设置的Master-Slave的复
             *                              制方式为同步复制时才可能出现这种异常状态。异步复制不会出现
             *      SLAVE_NOT_AVAILABLE, // 没有可用的Slave。当Broker集群设置为Master-Slave的
             *                              复制方式为同步复制时才可能出现这种异常状态。异步复制不会出现
             * }
             */
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
        }

        //关闭producer
        producer.shutdown();
    }
}

