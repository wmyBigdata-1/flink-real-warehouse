package com.wmy.flinkwmyinterface;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.wmy.flinkwmyinterface.mapper") // 这个是包名，四个接口，如果写全类名只能写一个了
public class FlinkWmyInterfaceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkWmyInterfaceApplication.class, args);
    }

}
