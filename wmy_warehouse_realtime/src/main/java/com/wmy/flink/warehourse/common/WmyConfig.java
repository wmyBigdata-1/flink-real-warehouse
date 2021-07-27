package com.wmy.flink.warehourse.common;

/**
 * ClassName:WmyConfig
 * Package:com.wmy.flink.warehourse.common
 *
 * @date:2021/7/15 18:13
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 常量类
 */
public class WmyConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "WMY_FLINK_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:yaxin01:2181";

    //ClickHouse驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    //ClickHouse连接地址
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://192.168.22.144:8123/default";
}
