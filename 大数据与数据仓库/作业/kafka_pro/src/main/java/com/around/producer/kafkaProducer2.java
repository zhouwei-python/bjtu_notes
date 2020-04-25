package com.around.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class kafkaProducer2 extends Thread{

    private String topic;
    private String path;
    public kafkaProducer2(String topic, String path){
        super();
        this.topic = topic;
        this.path = path;
    }
    public void run() {
        Properties properties = new Properties();
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.121:9092,192.168.43.122:9092,192.168.43.123:9092");
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put("zookeeper.connect", "192.168.43.121:2181,192.168.43.122:2181,192.168.43.123:2181");//声明zk
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        int i=0;
        String str = null;
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(path);
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            while((str = bufferedReader.readLine()) != null){
                i++;
                System.out.println(str+"-------"+i);
                producer.send(new ProducerRecord<String, String>(topic,str));
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        new kafkaProducer2("userskill","H:\\北交课程笔记\\大数据与数据仓库\\数据集\\数据仓库实验数据(1)\\某些数据\\userSkill\\userSkill").start();
        new kafkaProducer2("useredu","H:\\北交课程笔记\\大数据与数据仓库\\数据集\\数据仓库实验数据(1)\\某些数据\\userEdu\\userEdu").start();
    }
}
