package com.wmy.flink.warehourse.utils;

import com.wmy.flink.warehourse.bean.TransientSink;
import com.wmy.flink.warehourse.common.WmyConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName:ClickHouseUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/25 8:00
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: clickhouse 工具类
 */
public class ClickHouseUtil {
    // 流中的数据类型是：visitorStats
    public static <T> SinkFunction getSink(String sql) {
        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 封装的泛型也不知道是什么类型的
                        // 反射的方式来获取属性名
                        // javaBean，未来也想调用这个，有些javaBean的属性是不想调出去，在表里面没有，工具类中有，占位符不要
                        // 属性和表的列不对等，属性>=表的列
                        // 有的属性是不想写道clickhouse中
                        // 如何使用注解
                        // 定义一个计数，跳过的属性
                        int offset = 0;
                        Field[] fields = t.getClass().getDeclaredFields();// 权限问题，拿到所有的属性
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i]; // 获取具体的字段名，是需要把值放到字段里面
                            // 在字段中获取注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                offset++;
                                continue; // 这个就跳过操作，不需要进行赋值
                            }
                            // 如何获取值，反射的方式
                            field.setAccessible(true); // 可以访问私有属性的值的数据
                            try {
                                Object value = field.get(t); // 属性调对象，对象调属性
                                // 给占位符赋值
                                preparedStatement.setObject(i + 1 - offset, value);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(WmyConfig.CLICKHOUSE_DRIVER)
                        .withUrl(WmyConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
