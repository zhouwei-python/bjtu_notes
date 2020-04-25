package com.around.consumer;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class kafkaConsumer extends Thread{

    private String topic;

    public kafkaConsumer(String topic){
        super();
        this.topic = topic;
    }
    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        int i = 0;
        while(iterator.hasNext()){
            String message = new String(iterator.next().message());
            i++;
            System.out.println("接收到: " + message);
            String temp[]=message.split("\001");
            try {
                //1.注册数据库的驱动
                Class.forName("com.mysql.cj.jdbc.Driver");
                //2.获取数据库连接（里面内容依次是："jdbc:mysql://主机名:端口号/数据库名","用户名","登录密码"）
                Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.43.122:3306/kafka_pro?useUnicode=true&characterEncoding=utf-8&useSSL=false", "root", "root");
                //3.需要执行的sql语句（?是占位符，代表一个参数）
                String sql = "insert into userbasic values(?,?,?,?,?,?)";
                //4.获取预处理对象，并依次给参数赋值
                PreparedStatement statement = connection.prepareCall(sql);
                statement.setString(1,temp[0]); //数据库字段类型是int，就是setInt；1代表第一个参数
                statement.setString(2,temp[1]);    //数据库字段类型是String，就是setString；2代表第二个参数
                statement.setString(3,temp[2]); //数据库字段类型是int，就是setInt；3代表第三个参数
                statement.setString(4,temp[3]); //数据库字段类型是int，就是setInt；3代表第三个参数
                statement.setString(5,temp[4]); //数据库字段类型是int，就是setInt；3代表第三个参数
                statement.setString(6,temp[5]); //数据库字段类型是int，就是setInt；3代表第三个参数
                //5.执行sql语句（执行了几条记录，就返回几）
                int j = statement.executeUpdate();
                System.out.println(j);
                //6.关闭jdbc连接
                statement.close();
                connection.close();

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        System.out.println("共接收信息"+i);
    }
    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.121:9092,192.168.43.122:9092,192.168.43.123:9092");
        properties.put("zookeeper.connect", "192.168.43.121:2181,192.168.43.122:2181,192.168.43.123:2181");//声明zk
        properties.put("group.id", "bigdata");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) {
        new kafkaConsumer("userbasic").start();// 使用kafka集群中创建好的主题 test
    }

}






//import java.sql.*;
//import java.text.SimpleDateFormat;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import kafka.consumer.Consumer;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
//
//public class kafkaConsumer extends Thread{
//    private String topic;
//    public kafkaConsumer(String topic){
//        super();
//        this.topic = topic;
//    }
//    @Override
//    public void run() {
//        Properties properties = new Properties();
//        properties.put("zookeeper.connect", "192.168.43.121:2181,192.168.43.122:2181,192.168.43.123:2181");//声明zk
//        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "mcq");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
////        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.43.121:9092,192.168.43.122:9092,192.168.43.123:9092");
//        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));;
//        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
//        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
//        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
//        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
//        int i = 0;
//        while(iterator.hasNext()){
//            String message = new String(iterator.next().message());
//            i++;
//            System.out.println("接收到: " + message);
//            String temp[]=message.split("\001");
//            System.out.println(temp[0]+"-"+temp[1]+"-"+temp[2]+"-"+temp[3]+"-"+temp[4]+"-"+temp[5]+"-"+temp[6]+"-"+temp[7]);
////            try {
////
////                //1.注册数据库的驱动
////                Class.forName("com.mysql.cj.jdbc.Driver");
////                //2.获取数据库连接（里面内容依次是："jdbc:mysql://主机名:端口号/数据库名","用户名","登录密码"）
////                Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.43.122:3306/kafka_pro?useUnicode=true&characterEncoding=utf-8&useSSL=false","root","root");
////
////                //3.需要执行的sql语句（?是占位符，代表一个参数）
////                String sql = "insert into userbasic values('\"+temp[0]+\"','\"+temp[1]+\"','\"+temp[2]+\"','\"+temp[3]+\"','\"+temp[4]+\"','\"+temp[5]+\"')";
////                //4.获取预处理对象，并依次给参数赋值
////                PreparedStatement statement = connection.prepareCall(sql);
//////                statement.setString(1,"U000001"); //数据库字段类型是int，就是setInt；1代表第一个参数
//////                statement.setString(2,"male");    //数据库字段类型是String，就是setString；2代表第二个参数
//////                statement.setString(3,"1"); //数据库字段类型是int，就是setInt；3代表第三个参数
//////                statement.setString(4,"123"); //数据库字段类型是int，就是setInt；3代表第三个参数
//////                statement.setString(5,"222"); //数据库字段类型是int，就是setInt；3代表第三个参数
//////                statement.setString(6,"32"); //数据库字段类型是int，就是setInt；3代表第三个参数
////                //5.执行sql语句（执行了几条记录，就返回几）
////                int j = statement.executeUpdate();
////                System.out.println(j);
////                //6.关闭jdbc连接
////                statement.close();
////                connection.close();
////
////            } catch (ClassNotFoundException e) {
////                e.printStackTrace();
////            } catch (SQLException e) {
////                e.printStackTrace();
////            }
//
//
////            String sql = "insert into userbasic values('"+temp[0]+"','"+temp[1]+"','"+temp[2]+"','"+temp[3]+"','"+temp[4]+"','"+temp[5]+"')";
////            try {
////                Statement stmt1 = con.createStatement();
////                int rs3=stmt1.executeUpdate(sql);
////            } catch (SQLException e) {
////                e.printStackTrace();
////            }
//        }
//        System.out.println("共接收信息"+i);
//    }
//
//    public static void main(String[] args) {
//        new kafkaConsumer("userbasic").start();// 使用kafka集群中创建好的主题 test
//    }
//
//}
