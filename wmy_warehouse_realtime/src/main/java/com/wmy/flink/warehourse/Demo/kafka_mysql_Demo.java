package com.wmy.flink.warehourse.Demo;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * ClassName:kafka_mysql_Demo
 * Package:com.wmy.flink.warehourse.Demo
 *
 * @date:2021/7/20 11:25
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
public class kafka_mysql_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "yaxin01:9092");
        props.put("zookeeper.connect", "yaxin01:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<String>("student", new SimpleStringSchema(), props))
                .setParallelism(1)
                .map(str -> JSON.parseObject(str, Student.class));

        KafkaUtil.writeToKafka();
        student.addSink(new SinkToMySQL());

        env.execute("kafka_mysql_Demo >>> ");
    }
    public static class KafkaUtil {
        public static void writeToKafka() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "yaxin01:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
            ArrayList<Student> list = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Student student = new Student(i, "zhisheng" + i, "password" + i, 18 + i);
                list.add(student);
                if (list.size() >= 30) {
                    ProducerRecord<String, String> record = new ProducerRecord<>("kafka-mysql", null, JSON.toJSONString(student));
                    kafkaProducer.send(record);
                    System.out.println("send data: " + JSON.toJSONString(student));
                }
            }
            kafkaProducer.flush();
        }
    }


    public static class SinkToMySQL extends RichSinkFunction<Student> {

        PreparedStatement ps;
        Connection conn;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = getConnection();
            String sql = "insert into Student(id,name,password,age) values(?,?,?,?,?)";
            ps = this.conn.prepareStatement(sql);
        }

        @Override
        public void invoke(Student value, Context context) throws Exception {
            System.out.println("value : " + JSON.toJSONString(value));
            ps.setInt(1, value.getId());
            ps.setString(2, value.getName());
            ps.setString(3, value.getPasswprd());
            ps.setInt(4, value.getAge());
            ps.executeUpdate();
        }

        private Connection getConnection() {
            Connection conn = null;
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://yaxin01:3306/yaxin", "root", "000000");
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
            return conn;
        }

        @Override
        public void close() throws Exception {
            if (conn != null || ps != null) {
                try {
                    conn.close();
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Getter
    @Setter
    public static class Student {
        public int id;
        public String name;
        public String passwprd;
        public int age;
    }
}

