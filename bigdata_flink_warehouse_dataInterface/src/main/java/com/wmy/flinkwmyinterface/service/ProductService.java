package com.wmy.flinkwmyinterface.service;

import com.wmy.flinkwmyinterface.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName:ProductService
 * Package:com.wmy.flinkwmyinterface.service
 *
 * @date:2021/7/27 12:21
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
public interface ProductService {
    // 1、查询GMV总数
    // select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = 20210727;
    // 这个时间不能这样写：#(date)这个更方面，不用xml来进行关联了
    @Select("select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = #(date)")
    public BigDecimal getSumAmount(int date);

    List<ProductStats> getGmvByTm(@Param("date") int date, @Param("limit") int limit);
}
