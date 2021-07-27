package com.wmy.flink.warehourse.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName:KeywordStats
 * Package:com.wmy.flink.warehourse.bean
 *
 * @date:2021/7/25 10:25
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 关键词统计实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}

