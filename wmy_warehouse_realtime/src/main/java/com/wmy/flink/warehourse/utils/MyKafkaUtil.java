package com.wmy.flink.warehourse.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * ClassName:MyKafkaUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/13 16:46
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: kafka消费者的工具类
 */
public class MyKafkaUtil {

    private static String KAFKA_SERVER = "yaxin01:9092,yaxin02:9092,yaxin03:9092";
    private static Properties props = new Properties();
    private static final String DEFAULT_TOPIC = "dwd_default_topic";

    static {
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    }

    /**
     * 获取Kafka Source的方法
     * FlinkKafkaConsumer ---> FlinkKafkaConsumerBase ---> RichParallelSourceFunction
     * @param topic
     * @param groupID
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupID) {


        // 1、配置kafka消费参数
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);

        // 2、获取kafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000L + "");
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), props);
    }


    // 通用类
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000L + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic, String groupId) {
        return "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'" +
                ")";
    }
}
