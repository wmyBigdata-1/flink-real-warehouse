package com.wmy.flink.warehourse.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * ClassName:PaymentInfo
 * Package:com.wmy.flink.warehourse.bean
 *
 * @date:2021/7/23 7:07
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 支付表信息
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
    Long create_ts;
}
