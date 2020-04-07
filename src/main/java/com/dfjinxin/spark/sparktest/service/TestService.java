package com.dfjinxin.spark.sparktest.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TestService {

    @Autowired
    private SparkSession sparkSession;

    public void sql(String file){

//        SQLContext sqlContext = new SQLContext(sc);
//        Dataset filedata = sqlContext.read().json(file);
//        filedata.printSchema();
//        filedata.show();

        Dataset filedata = sparkSession.read().json(file);
        filedata.show();
//        sparkSession.stop();

    }
}
