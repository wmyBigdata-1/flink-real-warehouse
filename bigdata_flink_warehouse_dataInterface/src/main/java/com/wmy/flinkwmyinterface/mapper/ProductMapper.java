package com.wmy.flinkwmyinterface.mapper;

import com.wmy.flinkwmyinterface.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName:ProductMapper
 * Package:com.wmy.flinkwmyinterface.mapper
 *
 * @date:2021/7/27 12:20
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
public interface ProductMapper {
    // 1、查询GMV总数
    // select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = 20210727;
    // 这个时间不能这样写：#(date)这个更方面，不用xml来进行关联了
    @Select("select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = #{date})")
    public BigDecimal getSumAmount(int date);

    // 2、查询按照品牌分组的GMV
    @Select("select tm_name,sum(order_amount) order_amount from product_stats" +
            "where toYYYYMMDD(stt) = #{date}" +
            "group by tm_name" +
            "having order_amount > 0" +
            "order by order_amount desc" +
            "limit #{limit}")
    // 数据格式应该是什么样的，查出来的多行多列的，这个是如何进行处理的
    List<ProductStats> getGmvByTm(@Param("date") int date,@Param("limit") int limit);
}
