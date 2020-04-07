package com.dfjinxin.spark.sparktest.contoller;

import com.dfjinxin.spark.sparktest.service.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private TestService testService;

    @GetMapping("/spark/test")
    public String test(){
        System.out.println("TestController---------/spark/test");
        return "完成";
    }

    @GetMapping("/spark/sql")
    public String sql(@RequestParam("file") String file){
        testService.sql(file);
        return "完成";
    }
}
