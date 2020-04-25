package com.around.consumer;

//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//
//import java.util.Arrays;
//import java.util.Map;
//import java.util.Properties;
//
//public class MyConsumer {
//    public static void main(String[] args) {
//
//        //创建消费者配置信息
//        Properties properties = new Properties();
//        // 给配置信息赋值
//        // 连接的集群
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.122:9092,192.168.43.123:9092");
//        // 开启自动提交
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
//        // 自动提交的延时
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
//        // Key,Value反序列化
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//        // 消费者组
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata");
//        // 创建消费者
//        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
//        // 订阅主题
//        consumer.subscribe(String.valueOf(Arrays.asList("first","second")));
//        // 获取数据
//        while(true){
//            Map<String, ConsumerRecords<String, String>> consumerRecordsMap = consumer.poll(1000);
//            // 解析并打印consumerRecords
//            //消费消息，遍历records
//            for (ConsumerRecord<String, String> r : records) {
//                System.out.println(r.key() + ":" + r.value());
//            }
//        }
//
//    }
//}

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MyConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MyConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.122:9092,192.168.43.123:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS, "1000");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range");
//		properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(String.valueOf(Arrays.asList("first","second")));
        boolean isRunning = true;
        while(isRunning) {
            Map<String, ConsumerRecords<String, String>> results = kafkaConsumer.poll(100);
            if (null != results) {
                for (Map.Entry<String, ConsumerRecords<String, String>> entry : results.entrySet()) {
                    LOG.info("topic {}", entry.getKey());
                    ConsumerRecords<String, String> consumerRecords = entry.getValue();
                    List<ConsumerRecord<String, String>> records = consumerRecords.records();
                    for (int i = 0, len = records.size(); i < len; i++) {
                        ConsumerRecord<String, String> consumerRecord = records.get(i);
                        LOG.info("topic {} partition {}", consumerRecord.topic(), consumerRecord.partition());
                        try {
                            LOG.info("offset {} value {}", consumerRecord.offset(), new String(consumerRecord.value()));
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    }
                }
            }
        }

        kafkaConsumer.close();

    }

}