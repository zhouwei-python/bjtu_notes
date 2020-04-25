package com.around;

import java.sql.*;

public class ConnectDb {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {

            //加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");

            //创建连接
            //java10为数据库名
            String url="jdbc:mysql://192.168.43.121:3306/kafka_pro?useSSL=false&serverTimezone=UTC";
            String username="root";
            String userpwd="root";
            Connection conn = DriverManager.getConnection(url,username,userpwd);
            //3.需要执行的sql语句（?是占位符，代表一个参数）
            String sql = "insert into useredu(Uid,Degree,SchoolName,MajorStr) values(?,?,?,?)";
            //4.获取预处理对象，并依次给参数赋值
            PreparedStatement statement = conn.prepareCall(sql);
            statement.setString(1,"U000001"); //数据库字段类型是int，就是setInt；1代表第一个参数
            statement.setString(2,"小明");    //数据库字段类型是String，就是setString；2代表第二个参数
            statement.setString(3,"北京交通大学"); //数据库字段类型是int，就是setInt；3代表第三个参数
            statement.setString(3,"硕士"); //数据库字段类型是int，就是setInt；3代表第三个参数
            //5.执行sql语句（执行了几条记录，就返回几）
            int i = statement.executeUpdate();
            System.out.println(i);
            //6.关闭jdbc连接
            statement.close();
            conn.close();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
