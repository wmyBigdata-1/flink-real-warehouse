package com.wmy.flinkwmyinterface.service;

import com.wmy.flinkwmyinterface.bean.ProductStats;
import com.wmy.flinkwmyinterface.mapper.ProductMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName:ProductServiceImpl
 * Package:com.wmy.flinkwmyinterface.service
 *
 * @date:2021/7/27 12:21
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
// 这个是service的注解，可以跳一下报错的级别
@Service
public class ProductServiceImpl implements ProductService{

    // 这个是mybatis帮助我们做的:
    @Autowired
    ProductMapper productMapper;

    @Override
    public BigDecimal getSumAmount(int date) {
        return productMapper.getSumAmount(date);
    }

    @Override
    public List<ProductStats> getGmvByTm(int date, int limit) {
        return productMapper.getGmvByTm(date,limit);
    }
}
