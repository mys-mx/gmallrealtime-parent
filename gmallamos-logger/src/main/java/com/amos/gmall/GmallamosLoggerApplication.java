package com.amos.gmall;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


//Spring boot程序的入口类
//当SpringBoot程序执行的时候，会扫描同级别包下的 所有标记 类（Component），交给Spring进行管理
@SpringBootApplication
public class GmallamosLoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallamosLoggerApplication.class, args);
    }

}
