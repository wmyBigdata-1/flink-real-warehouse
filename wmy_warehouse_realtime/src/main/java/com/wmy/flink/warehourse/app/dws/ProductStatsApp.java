package com.wmy.flink.warehourse.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wmy.flink.warehourse.bean.OrderWide;
import com.wmy.flink.warehourse.bean.PaymentWide;
import com.wmy.flink.warehourse.bean.ProductStats;
import com.wmy.flink.warehourse.common.WmyConstant;
import com.wmy.flink.warehourse.func.DimAsyncFunction;
import com.wmy.flink.warehourse.utils.ClickHouseUtil;
import com.wmy.flink.warehourse.utils.DateTimeUtil;
import com.wmy.flink.warehourse.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * ClassName:ProductStatsApp
 * Package:com.wmy.flink.warehourse.app.dws
 *
 * @date:2021/7/25 9:25
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 商品主题
 */
public class ProductStatsApp {
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

        // 2、读取kafka数据，7个主题
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        FlinkKafkaConsumer<String> favorInfoSourceSource = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);

        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        // 3、将7个流转换成统一数据格式
        // 3.1 点击和曝光数据处理
        SingleOutputStreamOperator<ProductStats> clickAndDisplayDS = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                // 过来的数据就是页面数据，从JSON中获取page_id，转成JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                // 获取page_id,先获取page
                JSONObject pageObj = jsonObject.getJSONObject("page");
                String page_id = pageObj.getString("page_id");

                // 判断是否是good_detail，是否是点击数据
                if ("good_detail".equals(page_id)) {
                    ProductStats productStats = ProductStats
                            .builder()
                            .sku_id(pageObj.getLong("item")) // sku编号
                            .click_ct(1L) // 点击数
                            .build();
                    // 就不用向以前那么补很多零的操作
                    out.collect(productStats);
                }

                // 取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displaysObj = displays.getJSONObject(i);
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(displaysObj.getLong("item"))
                                .display_ct(1L)
                                .build();
                        out.collect(productStats);
                    }
                }
            }
        });

        // 3.2 收藏
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(
                json -> {
                    JSONObject favorInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(favorInfo.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(favorInfo.getLong("sku_id"))
                            .favor_ct(1L)
                            .ts(ts)
                            .build();
                });

        // 3.3 加购
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(
                json -> {
                    JSONObject cartInfo = JSON.parseObject(json);

                    Long ts = DateTimeUtil.toTs(cartInfo.getString("create_time"));

                    return ProductStats.builder()
                            .sku_id(cartInfo.getLong("sku_id"))
                            .cart_ct(1L)
                            .ts(ts)
                            .build();
                });

        // 3.4 下单
        SingleOutputStreamOperator<ProductStats> orderDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                // 转换为OrderWide对象
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);
                //获取时间戳字段
                Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());

                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_amount(orderWide.getTotal_amount()) // 下单商品金额
                        .order_sku_num(orderWide.getSku_num()) // 下单商品个数
                        .orderIdSet(new HashSet<>(Collections.singleton(orderWide.getOrder_id()))) // 用于统计订单数，这个数据不需要往clickhouse中去写的
                        .ts(ts)
                        .build();
            }
        });

        // 3.5 支付
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);

                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());

                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet<>(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                });

        // 3.6 退款
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(new HashSet<>(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                });

        // 3.7 评价
        SingleOutputStreamOperator<ProductStats> appraiseDS = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                //处理好评数
                Long goodCommentCt = 0L;
                if (WmyConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                    goodCommentCt = 1L;
                }

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCommentCt)
                        .ts(ts)
                        .build();
            }
        });

        // 4、Union
        DataStream<ProductStats> unionDS = clickAndDisplayDS.union(favorStatsDS,
                cartStatsDS,
                orderDS,
                paymentStatsDS,
                refundStatsDS,
                appraiseDS);

        // 5、设置Watermark（提取时间戳）
        SingleOutputStreamOperator<ProductStats> productStatsWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(
                        Duration.ofSeconds(10L)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // 6、分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWaterMarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));

                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct((long) stats1.getOrderIdSet().size());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());

                        stats1.setRefund_order_ct((long) stats1.getRefundOrderIdSet().size());
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        stats1.setPaid_order_ct((long) stats1.getPaidOrderIdSet().size());

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        long start = window.getStart();
                        long end = window.getEnd();

                        String stt = sdf.format(start);
                        String edt = sdf.format(end);

                        //取出聚合以后的数据
                        ProductStats productStats = input.iterator().next();

                        //设置窗口时间
                        productStats.setStt(stt);
                        productStats.setEdt(edt);

                        out.collect(productStats);
                    }
                });

        // 7、关联维度信息
        //7.1 关联SKU信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSku_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                //获取维度中的信息
                String sku_name = dimInfo.getString("SKU_NAME");
                BigDecimal price = dimInfo.getBigDecimal("PRICE");
                Long spu_id = dimInfo.getLong("SPU_ID");
                Long tm_id = dimInfo.getLong("TM_ID");
                Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                //关联SKU维度信息
                productStats.setSku_name(sku_name);
                productStats.setSku_price(price);
                productStats.setSpu_id(spu_id);
                productStats.setTm_id(tm_id);
                productStats.setCategory3_id(category3_id);

            }
        }, 300, TimeUnit.SECONDS);

        //7.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //打印测试
        productStatsWithTmDS.print();

        // 8、写入Clickhouse
        productStatsWithTmDS.addSink(ClickHouseUtil.getSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // 9、执行任务
        env.execute("ProductStatsApp >>>> ");
    }
}
