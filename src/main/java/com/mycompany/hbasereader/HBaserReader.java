/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hbasereader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.api.java.JavaSQLContext;
//import org.apache.spark.sql.api.java.JavaSchemaRDD;

/**
 *
 * @author rakib
 */
public class HBaserReader {

    public static void main(String[] args) throws IOException {

        HTable table = HBaseManager.getHBaseManager().createHTable("2017100710_tmp");
        ResultScanner scanner = table.getScanner(new Scan());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("HBaseReader").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        

        List<String> jsonList = new ArrayList<String>();

        String json = null;

        for (Result rowResult : scanner) {
            json = "";
            String rowKey = Bytes.toString(rowResult.getRow());
            //             String rowValue = Bytes.toString(rowResult.getValue("method", "method_data"));
//            System.out.println(rowKey);
            for (byte[] s1 : rowResult.getMap().keySet()) {

                String s1_str = Bytes.toString(s1);
//                System.out.println("Colume Family:" + s1_str);
                String jsonSame = "";

                for (byte[] s2 : rowResult.getMap().get(s1).keySet()) {
                    String s2_str = Bytes.toString(s2);
//                    System.out.println("Quantify:" + s2_str);

                    for (long s3 : rowResult.getMap().get(s1).get(s2).keySet()) {
                        String s3_str = new String(rowResult.getMap().get(s1).get(s2).get(s3));
//                        System.out.println("Value:" + s3_str);
                        jsonSame += "\"" + s2_str + "\":" + s3_str + ",";

                    }
                }
                jsonSame = jsonSame.substring(0, jsonSame.length() - 1);
                json += "\"" + s1_str + "\"" + ":{" + jsonSame + "}" + ",";
            }
            json = json.substring(0, json.length() - 1);
            json = "{\"RowKey\":\"" + rowKey + "\"," + json + "}";
            jsonList.add(json);
        }
        JavaRDD<String> jsonRDD = sc.parallelize(jsonList);
        //  JavaSchemaRDD schemaRDD = jsql.jsonRDD(jsonRDD);
        jsonRDD.foreach(p -> {
            System.out.println(p);
        });
        
       

//        JavaSchemaRDD schemaRDD = jsql.jsonRDD(jsonRDD);
//         System.out.println(schemaRDD.take(2));
    }

}
