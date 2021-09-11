package com.example.controller;

import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @program: java2python
 * @description:
 * @create: 2021-09-11 13:39
 */

@RestController
public class TestController {


    @GetMapping("test")
    public String test() {
        PythonInterpreter interpreter = new PythonInterpreter();
        interpreter.execfile("D:\\pyWorkSpace\\opencv\\test.py");
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
