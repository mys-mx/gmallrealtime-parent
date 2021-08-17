package com.amos.gmall.controller;

import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2021-08-14 10:38
 * @Component 对当前类进行标记，SpringBoot程序启动后，会扫描到这个类
 * 将这个类对象的创建以及对象之间关系的维护交给Spring容器进行管理
 * <p>
 * >@Controller
 * >@Service
 * >@Repository
 * @RequestMapping 接收什么样的请求，并进行响应
 * @如果使用的是Controller注解，那么类中的方法返回值是String类型，那么返回值表示跳转页面的路径
 * @ResponseBody 将返回值以字符串的形式返回给客户端
 *
 * @RestController = @Controller + @ResponseBody
 */

@RestController
public class FirstController {
    //    处理客户端的请求，并且进行响应
    @RequestMapping("/first")
    public String test() {
        return "this is first";
    }
}
