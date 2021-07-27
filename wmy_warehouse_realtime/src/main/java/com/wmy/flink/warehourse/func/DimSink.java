package com.wmy.flink.warehourse.func;

import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.common.WmyConfig;
import com.wmy.flink.warehourse.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * ClassName:DimSink
 * Package:com.wmy.flink.warehourse.func
 *
 * @date:2021/7/17 9:48
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 自定义HBASE Sink
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化phoenix连接
        Class.forName(WmyConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(WmyConfig.PHOENIX_SERVER);
    }

    // 将数据写入phoenix：upsert into t(id,name,sex) values(......);
    // 拼接SQL
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            // 获取数据中的key以及value
            JSONObject data = value.getJSONObject("data");
            Set<String> keys = data.keySet();
            Collection<Object> values = data.values();

            // 拼接SQL，获取表名
            String tableName = value.getString("sink_table");

            // 创建插入 数据的SQL
            String upsertSql = genUpsertSql(tableName, keys, values);

            // 打印插入语句的SQL是否正确
            System.out.println("插入语句的SQL >>>> " + upsertSql);

            // 编译SQL
            preparedStatement = conn.prepareStatement(upsertSql);

            // 执行
            preparedStatement.executeUpdate();

            // 提交: close也会做提交
            conn.commit();

            // 判断如果更新操作，则删除Redis中的数据保证数据的一致性
            String type = value.getString("type");
            if ("update".equals(type)) {
                DimUtil.deleteCached(tableName, data.getString("id")); // id：是数据主键对应的值
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            System.out.println("插入phoenix数据失败。。。");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * 创建SQL的方法: upsert into t(id,name,sex) values('','','')
     * @param tableName
     * @param keys
     * @param values
     * @return
     */
    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + WmyConfig.HBASE_SCHEMA + "." +
                tableName + "(" + StringUtils.join(keys, ",") + ")" +
                " values('" + StringUtils.join(values, "','") + "')";
    }
}
