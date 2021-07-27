package com.wmy.flink.warehourse.func;

import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.utils.DimUtil;
import com.wmy.flink.warehourse.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * ClassName:DimAsyncFunction
 * Package:com.wmy.flink.warehourse.func
 *
 * @date:2021/7/22 9:21
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 异步查询
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{
    // 声明查询的表名
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    // 声明线程池对象
    private ThreadPoolExecutor threadPoolExecutor;

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // 写异步提交的客户端
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // 0、获取查询条件字段
                String key = getKey(input);

                // 1、查询维度信息 ---> 表名的从构造方法中传过去
                // 字段的信息如何去传递勒，关联维度的字段如何办，反射获取，
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);

                // 2、关联到事实表数据上
                if (dimInfo != null && dimInfo.size() > 0) {
                    // 这个也是抽象方法
                    join(input,dimInfo);
                }

                // 3、继续向下游数据传输
                // 这个是核心固定的一个方法，只能是按照这个模式来进行优化就行
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
}
