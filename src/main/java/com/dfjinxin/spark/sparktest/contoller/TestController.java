package com.dfjinxin.spark.sparktest.contoller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @GetMapping("/spark/test")
    public String test(){
        System.out.println("TestController---------/spark/test");
        return "完成";
    }
}
