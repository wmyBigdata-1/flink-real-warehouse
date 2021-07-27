package com.wmy.flink.warehourse.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * ClassName:TransientSink
 * Package:com.wmy.flink.warehourse.bean
 *
 * @date:2021/7/25 8:29
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 自定义对象的sink bean
 */
@Target(FIELD) // 在字段上
@Retention(RUNTIME) // 什么时候生效
public @interface TransientSink {
}
