package com.example.javaquotepython.controller;

import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @program: java2python
 * @description:
 * @create: 2021-09-11 13:39
 */

@RestController
public class TestController {

    @GetMapping ("/openLight")
    public String test() {

        Properties props = new Properties();
        props.put("python.home", "/user/local/anaconda-py3/bin/python3");
//        props.put("python.home", "D:\\GoogleDownLoads\\python\\python");
        props.put("python.console.encoding", "UTF-8");
        props.put("python.security.respectJavaAccessibility", "false");
        props.put("python.import.site", "false");
        Properties properties = System.getProperties();
        PythonInterpreter.initialize(properties, props, new String[0]);

        PythonInterpreter interpreter = new PythonInterpreter();
        System.out.println("---------------interpreter start--------------------");
        interpreter.execfile("/root/test.py");
//        interpreter.execfile("D:\\pyWorkSpace\\opencv\\test.py");
        System.out.println("---------------interpreter end--------------------");


        PyFunction function = interpreter.get("quick_sort", PyFunction.class);

        List<Integer> list = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            list.add(random.nextInt(100));
        }

        PyObject pyObject = function.__call__(new PyList(list), new PyInteger(0), new PyInteger(list.size() - 1));
        return pyObject.toString();
    }


}
