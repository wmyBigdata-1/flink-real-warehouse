package com.wmy.flink.warehourse.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * ClassName:canal_demo
 * Package:com.wmy.flink.warehourse.canal
 *
 * @date:2021/7/14 18:26
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: CS架构
 */
public class canal_demo {
    public static void main(String[] args) {
        // 1. 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.22.140",
                11111), "example", "canal", "123456");

        // 指定一次性获取数据的条数
        int batchSize = 5 * 1024;
        boolean running = true;

        try {
            while(running) {
                // 2. 建立连接
                connector.connect();
                // 回滚上次的get请求，重新获取数据
                connector.rollback();
                // 订阅匹配日志
                connector.subscribe("gmall.*");
                while(running) {
                    // 批量拉取binlog日志，一次性获取多条数据

                    Message message = connector.getWithoutAck(batchSize);
                    // 获取batchId
                    long batchId = message.getId();
                    // 获取binlog数据的条数
                    int size = message.getEntries().size();
                    if(batchId == -1 || size == 0) {

                    }
                    else {
                        printSummary(message);
                    }
                    // 确认指定的batchId已经消费成功
                    connector.ack(batchId);
                }
            }
        } finally {
            // 断开连接
            connector.disconnect();
        }
    }

    private static void printSummary(Message message) {
        // 遍历整个batch中的每个binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 事务开始
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventTypeName = entry.getHeader().getEventType().toString().toLowerCase();

            System.out.println("logfileName" + ":" + logfileName);
            System.out.println("logfileOffset" + ":" + logfileOffset);
            System.out.println("executeTime" + ":" + executeTime);
            System.out.println("schemaName" + ":" + schemaName);
            System.out.println("tableName" + ":" + tableName);
            System.out.println("eventTypeName" + ":" + eventTypeName);

            CanalEntry.RowChange rowChange = null;

            try {
                // 获取存储数据，并将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 判断是否为删除事件
                if(entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("---delete---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                }
                // 判断是否为更新事件
                else if(entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("---update---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                    printColumnList(rowData.getAfterColumnsList());
                }
                // 判断是否为插入事件
                else if(entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("---insert---");
                    printColumnList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
            }
        }
    }

    // 打印所有列名和列值
    private static void printColumnList(List<CanalEntry.Column> columnList) {
        for (CanalEntry.Column column : columnList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }
}