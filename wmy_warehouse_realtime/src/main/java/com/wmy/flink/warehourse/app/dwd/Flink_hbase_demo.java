package com.wmy.flink.warehourse.app.dwd;

import com.wmy.flink.warehourse.common.WmyConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName:Flink_hbase_demo
 * Package:com.wmy.flink.warehourse.app.dwd
 *
 * @date:2021/7/15 21:42
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description:
 */
public class Flink_hbase_demo {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "WMY_FLINK_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:yaxin01:2181";

    //定义Phoenix的连接
    private static Connection connection = null;

    public static void main(String[] args) {

        //初始化Phoenix的连接
        try {
            Class.forName(PHOENIX_DRIVER);
            connection = DriverManager.getConnection(PHOENIX_SERVER);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //执行建表SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement("create table if not exists test(id varchar primary key,name varchar,sex varchar)");
            preparedStatement.execute();
            System.out.println("创建表成功。。。");

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }




    }
}
