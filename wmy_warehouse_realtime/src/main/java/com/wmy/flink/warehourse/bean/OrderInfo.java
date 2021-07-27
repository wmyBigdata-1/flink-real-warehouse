package com.wmy.flink.warehourse.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName:OrderInfo
 * Package:com.wmy.flink.warehourse.bean
 *
 * @date:2021/7/20 9:08
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 订单信息
 */
@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;
}
