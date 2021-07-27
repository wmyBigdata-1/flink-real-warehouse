package com.wmy.flink.warehourse.bean;

import lombok.Data;

/**
 * ClassName:TableProcess
 * Package:com.wmy.flink.warehourse.bean
 *
 * @date:2021/7/14 10:59
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: java Bean操作
 */

@Data
public class TableProcess {
    // 动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    private String sourceTable; // 来源表
    private String operateType; // 操作类型：insert,update,delete
    private String sinkType; // 输出类型：hbase,kafka
    private String sinkTable; // 输出表（主题）
    private String sinkColumns; // 输出字段
    String sinkPk;//主键字段
    private String sinkExtend; // 建表扩展
}
