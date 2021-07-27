package com.wmy.flink.warehourse.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.bean.VisitorStats;
import com.wmy.flink.warehourse.utils.ClickHouseUtil;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * ClassName:VisitorStatsApp
 * Package:com.wmy.flink.warehourse.app.dws
 *
 * @date:2021/7/24 7:24
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 访客主题宽表的计算
 * <p>
 * Mock ---> ngix ---> Logger ---> kafka(ods_base_log) ---> flinkApp(LogBaseApp) dwd_page_log\dwd_start_log\dwd_display_log
 * FlinkApp(DayAcitilyUserCountApp UserJumpDetailApp) ---> kafka(dwm_unique_visit\dwm_user_jum_detail) ---> FlinkApp(VisitorStatsApp)
 * <p>
 * hadoop zk kafka hbase redis maxwell mock log
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 设置状态后端
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem.getUnguardedFileSystem(new URI("hdfs://yaxin01:9820:/flink-realtime-warehouse/dws_log/ck"));

        // 1.2 开启检查点
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);

        // 1.3 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 1.4 设置重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2、读取kafka主题的数据
        // dwd_page_log(PV、访问时长、进入页面数)
        // dwm_unique_visit(UV)
        // dwm_user_jum_detail(跳出数)
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log"; // LogBaseApp
        String uniqueVisitSourceTopic = "dwm_unique_visit"; // DayAcitilyUserCountApp
        String userJumpDetailSourceTopic = "dwm_user_jum_detail"; // UserJumpDetailApp

        // 擅长使用工具类来减少实时开发
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        // 测试打印
        pageLogDS.print("pageLogDS >>>> ");
        uvDS.print("uvDS >>>> ");
        userJumpDS.print("userJumpDS >>>> ");

        // 3、格式化流的数据，使其字段统一（JavaBean）
        // 3.1 将页面流格式化为VisitorStats ---> PV和访问时长
        SingleOutputStreamOperator<VisitorStats> pvAndDtDS = pageLogDS.map(jsonStr -> {
            // 转换成JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    0L, // 独立访客数
                    1L, // 页面访问数
                    0L, // 进入次数
                    0L, // 跳出次数
                    jsonObject.getJSONObject("page").getLong("during_time"), // 访问时长
                    jsonObject.getLong("ts")
            );
            // 三个主题，四个流，五个指标
        });

        // 3.2 将页面数据流像过滤格式化为VisitorStats ---> sv_ct（进入次数）
        SingleOutputStreamOperator<VisitorStats> svCountDS = pageLogDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<VisitorStats> out) throws Exception {
                // 将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                // 先获取上一跳的页面数据
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPage == null || lastPage.length() <= 0) { // 没有上一跳，第一次进入平台的数据
                    JSONObject commonObj = jsonObject.getJSONObject("common");
                    out.collect(new VisitorStats(
                                    "",
                                    "",
                                    commonObj.getString("vc"),
                                    commonObj.getString("ch"),
                                    commonObj.getString("ar"),
                                    commonObj.getString("is_new"),
                                    0L, // 没有关系的都补零
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObject.getLong("ts")
                            )
                    );
                }
            }
        });

        // 3.3 将uvDS格式化为VisitorStats ---> UV
        SingleOutputStreamOperator<VisitorStats> uvCountDS = uvDS.map(jsonStr -> {
            // 将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        // 3.4 将userJumpDS格式化为VisitorStats ---> uj_ct(跳出次数)
        SingleOutputStreamOperator<VisitorStats> ujCountDS = userJumpDS.map(jsonStr -> {
            // 将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        // 4、将多个流的数据进行union
        DataStream<VisitorStats> unionDS = pvAndDtDS.union(svCountDS, uvCountDS, ujCountDS);

        // 5、分组、聚合计算
        // 5.1 聚合计算
        // 应该调用什么算子：把五个指标给累加起来,可以使用reduce
        // 数据量太大 ---> 来自于四个流的数据，clickhouse的数据流的更新，更新操作没有那么快
        // 统计连续增长的指标，涉及四个流的数据，但凡有一条数据流来了，clickhouse都会更新操作，如果才能降低数据量，可以在聚合的时候使用开窗，使用窗口函数做过滤
        // 5秒的数据，一组key一条，对于某一个key（四个字段），出一条数据，clickhouse中的压力就减少很多
        // 维度最细粒度，把所有指标的数据都用到，这样实现复用的操作，维度选择，一般都是产品固定的，指标，需要什么求什么就好了
        // 窗口的开始和结束时间
        // 追求实时性，还是性能，空间和时间，clickhouse10秒更新一次
        // 阿里的双11，增加了很多机器，clickhouse接受的请求太多了，实时性就比较弱一点
        // 提取事件时间
        SingleOutputStreamOperator<VisitorStats> timestampsAndWatermarks = unionDS.assignTimestampsAndWatermarks
                (WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                );

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream =
                timestampsAndWatermarks.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
                    }
                });

        // 5.2 开窗，现在都是使用window
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow>
                windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // reduce（不能转换类型，输入和输出类型一样）和aggregate(可以转换类型，输入和输出类型可以不一样的)
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        // value1 和 value2 前面的四个字段都是一样的
                        return new VisitorStats(
                                "",
                                "",
                                value1.getVc(),
                                value1.getCh(),
                                value1.getAr(),
                                value1.getIs_new(),
                                // 这个做累加了，下面的五个指标
                                value1.getUv_ct() + value2.getUv_ct(),
                                value1.getPv_ct() + value2.getPv_ct(),
                                value1.getSv_ct() + value2.getSv_ct(), // 进入次数
                                value1.getUj_ct() + value2.getUj_ct(), // 跳出次数
                                value1.getDur_sum() + value2.getDur_sum(),
                                System.currentTimeMillis() // 统计时间，这里传入一个系统时间
                        );
                    }
                }
                , new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                                      TimeWindow window, Iterable<VisitorStats> input,
                                      Collector<VisitorStats> out) throws Exception {
                        // 首先取出数据
                        VisitorStats visitorStats = input.iterator().next();

                        // 取出窗口的开始和结束时间
                        long start = window.getStart();
                        long end = window.getEnd();

                        // 将数据处理成秒
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        String stt = simpleDateFormat.format(start);
                        String edt = simpleDateFormat.format(end);

                        // 设置时间数据
                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        // 将数据给写出
                        out.collect(visitorStats);
                    }
                }
        );

        // 测试汇入的结果集
        result.print("resultDS >>>> ");

        // 6、将聚合之后的数据写入clickhouse ---> 是否支持去重
        result.print("result >>>> ");
        result.addSink(ClickHouseUtil.<VisitorStats>getSink("insert into tableName values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // 7、执行任务
        env.execute("VisitorStatsApp >>>> ");
    }
}
