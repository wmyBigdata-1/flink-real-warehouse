package com.wmy.flink.warehourse.func;

import com.alibaba.fastjson.JSONObject;

/**
 * ClassName:DimJoinFunction
 * Package:com.wmy.flink.warehourse.func
 *
 * @date:2021/7/22 10:16
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 维度join
 */
public interface DimJoinFunction<T> {
    // 类在后面，方法的泛型是在后面
    // 获取数据中的维度数据中所要关联维度的主键，简单就是也给查询条件
    String getKey(T input);

    // 关联事实数据和维度数据
    // 记住不管是我们在进行接口的设置还是抽象的时候，最好要设置异常的范围，要不然重写的时候是无法进行抛出异常的
    void join(T input, JSONObject dimInfo) throws Exception;
}
