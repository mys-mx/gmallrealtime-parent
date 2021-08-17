package com.amos.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

/**
 * @program: gmallrealtime-parent
 * @description: 日志处理服务
 * @create: 2021-08-15 10:16
 */


@RestController
@Slf4j
public class LoggerController {

    // spring框架集成的kafka 类
    @Autowired //注入
            KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String jsonLog) {
        //1.打印输出到控制台
        // System.out.println(jsonLog);

        //2.落盘
        log.info(jsonLog);

        //3.生成日志发送到kafka对应的主题
        /*  Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop01:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.send(new ProducerRecord<>("topic",jsonLog));*/


        kafkaTemplate.send("ods_base_log", jsonLog);

        return "success!!";
    }
}
