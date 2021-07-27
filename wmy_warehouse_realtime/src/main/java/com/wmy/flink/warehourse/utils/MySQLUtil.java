package com.wmy.flink.warehourse.utils;

import com.google.common.base.CaseFormat;
import com.wmy.flink.warehourse.bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName:MySQLUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/14 10:57
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 读取MySQL中的配置信息，返回值类型是ResultSet，可以封装成对象，对于以后取数据方便 ---> 对象关系映射
 */
public class MySQLUtil {
    public static <T> List<T> queryList(String sql,Class<T> cls,boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            // 1、注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 2、获取连接
            conn = DriverManager.getConnection("jdbc:mysql://yaxin01:3306/gmall?characterEncoding=utf-8&userSSL=false", "root", "000000");
            // 3、编译SQL、并给占位赋值
            preparedStatement = conn.prepareStatement(sql);
            // 4、执行查询
            resultSet = preparedStatement.executeQuery();
            // 5、解析查询
            ArrayList<T> list = new ArrayList<>();
            // 取出列的元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                // 封装java bean 并加入集合
                T t = cls.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    // 获取列名
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    Object object = resultSet.getObject(i);
                    //给JavaBean对象赋值
                    BeanUtils.setProperty(t, columnName, object);
                }
                list.add(t);
            }
            // 6、返回结果
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询配置信息失败！！！");
        }finally {
            //7.释放资源
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        System.out.println(tableProcesses);

    }
}
