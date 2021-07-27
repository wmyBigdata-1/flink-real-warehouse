package com.wmy.flink.warehourse.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * ClassName:DateTimeUtil
 * Package:com.wmy.flink.warehourse.utils
 *
 * @date:2021/7/23 8:08
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 时间日期工具类
 */
public class DateTimeUtil {
    private final static DateTimeFormatter formator = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 时间格式转字符串格式的
    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formator.format(localDateTime);
    }

    // 字符串格式转时间格式的时间和日期
    public static Long toTs(String YMDhms){
        LocalDateTime localDateTime = LocalDateTime.parse(YMDhms, formator);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static void main(String[] args) {
        System.out.println(toYMDhms(new Date()));
        System.out.println(toTs("2021-07-23 08:13:22"));
    }
}
