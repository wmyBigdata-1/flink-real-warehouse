package com.wmy.flink.warehourse.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * ClassName:RedisUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/21 10:29
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
public class RedisUtil {
    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); // 最大得可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); // 连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); // 等待时间
            jedisPoolConfig.setMaxIdle(5); // 最大得闲置连接数
            jedisPoolConfig.setMinIdle(5); // 最小得闲置连接数
            jedisPoolConfig.setTestOnBorrow(true); // 取连接得时候镜像一下测试ping pong

            jedisPool = new JedisPool(jedisPoolConfig, "yaxin01", 6379, 1000);
            System.out.println("开辟连接池。。。");
            return jedisPool.getResource();
        } else {
            return jedisPool.getResource();
        }
    }

}
