package com.wmy.flink.warehourse.utils;

import com.wmy.flink.warehourse.common.WmyConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName:PhoenixUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/21 9:31
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
public class PhoenixUtil {
    // 声明
    private static Connection conn;

    // 初始化连接
    private static Connection init() {
        try {
            Class.forName(WmyConfig.PHOENIX_DRIVER);
            Connection conn = DriverManager.getConnection(WmyConfig.PHOENIX_SERVER);
            conn.setSchema(WmyConfig.HBASE_SCHEMA);
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败。。。");
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> cls) {
        // 初始化连接
        if (conn == null) {
            conn = init();
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            preparedStatement = conn.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();

            // 获取查询结果
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            ArrayList<T> list = new ArrayList<>();

            while (resultSet.next()) {
                T t = cls.newInstance();
                for (int i = 0; i < columnCount; i++) {
                    BeanUtils.setProperty(t, metaData.getColumnName(i), resultSet.getObject(i));
                }
                list.add(t);
            }
            return list;
        } catch (Exception throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("查询维度失败。。。");
        }finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }
}
