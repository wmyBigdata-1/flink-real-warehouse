package com.wmy.flink.warehourse.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.List;

/**
 * ClassName:DimUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/21 9:45
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: select * from t where id='19' and name='zhangsan';
 * and 怎么办，如果一个条件不需要and
 *
 * redis :
 *  1、存什么数据 维度数据 JSON Str
 *  2、用什么类型 String Set Hash List
 *  3、redis key 的设计？ 表名+主键 ---> 表名:列名:值
 */
public class DimUtil {
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {
        if (columnValues.length <= 0) {
            throw new RuntimeException("查询维度数据时，请至少设置一个查询条件。。。");
        }
        StringBuilder wheresql = new StringBuilder(" where ");

        // 创建RedisKey
        StringBuilder redisKey = new StringBuilder(tableName).append(":");

        // 遍历查询条件并赋值whereSql
        for (int i = 0; i < columnValues.length; i++) {
            Tuple2<String, String> columnValue = columnValues[i];
            String column = columnValue.f0;
            String value = columnValue.f1;
            wheresql.append(column).append("='").append(value).append("'");

            redisKey.append(value);

            // 如果不是最后一个条件，则添加为and
            if (i < columnValues.length - 1) {
                wheresql.append(" and ");
                redisKey.append(":");
            }
        }

        // 创建Phoenix where子句
        String querySql = "select * from " + tableName + wheresql.toString();

        // 获取Redis连接
        Jedis jedis = RedisUtil.getJedis();
        String dimJsonStr = jedis.get(redisKey.toString());

        // 判断是否从Redis中查询到数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            jedis.close();
            return JSON.parseObject(dimJsonStr);
        }

        // 查询Phoenix中的维度数据
        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class);

        // 将数据写入到Redis中
        jedis.set(redisKey.toString(), queryList.get(0).toString());

        // 设置过期时间
        jedis.expire(redisKey.toString(), 24 * 60);
        return queryList.get(0);
    }

    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    // 根据key让Redis中的缓存失效
    public static void deleteCached(String tableName, String id) {
        String key = tableName.toUpperCase() + ":" + id; // 注意这里的键值对时必须要统一的
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", new Tuple2<>("id", "19")));
        long first = System.currentTimeMillis();
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", new Tuple2<>("id", "19")));
        long end = System.currentTimeMillis();
        System.out.println(first - start);
        System.out.println(end - first);
    }
}
