package com.wmy.flinkwmyinterface.controller;

import com.wmy.flinkwmyinterface.bean.ProductStats;
import com.wmy.flinkwmyinterface.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * ClassName:SugarController
 * Package:com.wmy.flinkwmyinterface.controller
 *
 * @date:2021/7/27 11:53
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
/**
 * Desc: 大屏展示的控制层
 * 主要职责：接收客户端的请求(request)，对请求进行处理，并给客户端响应(response)
 *
 * @RestController = @Controller + @ResponseBody
 * @RequestMapping()可以加在类和方法上 加在类上，就相当于指定了访问路径的命名空间
 */
//@Controller // 这个是返回页面，页面报404，找不到对象
@RestController // @Controller + @ResponseBody
@RequestMapping("/api/sugar") // 最终的样子：/api/sugar/gmv
public class SugarController {

    @Autowired
    ProductService productService;

    /*
        -请求地址
		$API_HOST/api/sugar/trademark?limit=5

	-返回数据的格式
		{
		  "status": 0,
		  "data": {
		    "categories": ["苹果","三星","华为"],
		    "series": [
		      {
		        "data": [9387,8095,8863]
		      }
		    ]
		  }
		}
     */
    /*
    方式1：使用字符串拼接的方式处理返回的json数据
    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(
        @RequestParam(value = "date", defaultValue = "0") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        //如果没有传递日期参数，那么将日期设置为当前日期
        if (date == 0) {
            date = now();
        }
        //调用service根据品牌获取交易额排名
        List<ProductStats> productStatsByTrademarkList = productStatsService.getProductStatsByTrademark(date, limit);

        //定义两个集合，分别存放品牌的名称以及品牌的交易额
        List<String> trademarkNameList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();

        //对获取到的品牌交易额进行遍历
        for (ProductStats productStats : productStatsByTrademarkList) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());

        }
        String json = "{" +
            "\"status\": 0," +
            "\"data\": {" +
            "\"categories\": [\"" + StringUtils.join(trademarkNameList, "\",\"") + "\"]," +
            "\"series\": [" +
            "{" +
            "\"data\": [" + StringUtils.join(amountList, ",") + "]" +
            "}]}}";

        return json;
    }*/

    //方式2：封装对象，通过将对象转换的json格式字符串的方式 返回json数据
    @RequestMapping("/trademark")
    public Map getGmvByTm(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        //如果没有传递日期参数，那么将日期设置为当前日期
        if (date == 0) {
            date = now();
        }
        //调用service根据品牌获取交易额排名
        List<ProductStats> productStatsByTrademarkList = productService.getGmvByTm(date, limit);

        //定义两个集合，分别存放品牌的名称以及品牌的交易额
        List<String> trademarkNameList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();

        //对获取到的品牌交易额进行遍历
        for (ProductStats productStats : productStatsByTrademarkList) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());

        }
        Map resMap = new HashMap();
        resMap.put("status", 0);
        Map dataMap = new HashMap();
        dataMap.put("categories", trademarkNameList);
        List seriesList = new ArrayList();
        Map seriesDataMap = new HashMap();
        seriesDataMap.put("data", amountList);
        seriesList.add(seriesDataMap);
        dataMap.put("series", seriesList);
        resMap.put("data", dataMap);
        return resMap;
    }

    /**
     * 查的表和内容是不一样，其实接口是很难的
     * {
     * "status": 0,
     * "msg": "",
     * "data": 1201083.13565484
     * }
     */
    @RequestMapping("/gmv")
    //@ResponseBody
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = now(); // 今天的时间
        }
        return "{" +
                "\"status\": 0," +
                "\"msg\": \"\"," +
                "\"data\": " + productService.getSumAmount(date) + "" + // 查询的值，如果默认值可以改成今天
                "}";
    }

    private int now() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(new Date()));
    }
}
