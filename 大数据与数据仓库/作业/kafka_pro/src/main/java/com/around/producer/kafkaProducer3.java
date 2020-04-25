// 模拟小时级（每小时一次）数据抓取的 Kakfa 生产者
package com.around.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class kafkaProducer3 extends Thread{

    private String topic;
    private String path;
    public kafkaProducer3(String topic, String path){
        super();
        this.topic = topic;
        this.path = path;
    }

    public void run() {
        Properties properties = new Properties();
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.121:9092,192.168.43.122:9092,192.168.43.123:9092");
        properties.put("zookeeper.connect", "192.168.43.121:2181,192.168.43.122:2181,192.168.43.123:2181");//声明zk
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms", 1000); //选择批量发送的时间间隔

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        int i=0;
        String str = null;
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(path);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            while((str = bufferedReader.readLine()) != null){
                i++;
                System.out.println(str+"-------"+i);
                producer.send(new ProducerRecord<String, String>(topic,str));
                try {
                    TimeUnit.SECONDS.sleep(3600);  //这里设置为一小时
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        new kafkaProducer3("articleinfo","H:\\北交课程笔记\\大数据与数据仓库\\数据集\\数据仓库实验数据(1)\\某些数据\\articleInfo\\articleInfo").start();
    }
}
