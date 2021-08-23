package com.amos.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.amos.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;

/**
 * @program: gmallrealtime-parent
 * @description: 写出维度数据sink实现类
 * @create: 2021-08-22 15:48
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {

        //对连接对象进行初始化
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    //对流中数据进行处理
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取目标表名称
        String sinkTable = jsonObj.getString("sink_table");

        //保留的业务表中的字段
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            //根据data中属性名和属性值 生成upsert语句
            String upsertSql = genUpsertSql(sinkTable.toUpperCase(), dataJsonObj);
            System.out.println("向phoenix插入数据的SQL：" + upsertSql);
            PreparedStatement preparedStatement = null;
            try {
                //执行SQL
                preparedStatement = conn.prepareStatement(upsertSql);
                preparedStatement.executeUpdate();

                //注意：执行完phoenix插入操作之后需要手动提交事务
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向phoenix插入数据失败");
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }

            }

        }

    }

    // 根据data属性和值  生成phoenix中插入数据的sql语句
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {


        Set<String> keys = dataJsonObj.keySet();

        Collection<Object> values = dataJsonObj.values();

        //数据量比较少用String  数据量比较大还要保证效率StringBuilder  线程安全的StringBuffer
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                StringUtils.join(keys, ",") + ") ";

        String valueSql = "values ('" + StringUtils.join(values, "','") + "')";

        return sql + valueSql;
    }

    public static void main(String[] args) {
        DimSink dimSink = new DimSink();
        System.out.println("{" + "id" + ":1," + "name:" + "'zs'}");
        String test = dimSink.genUpsertSql("test", JSONObject.parseObject("{" + "id" + ":1," + "name:" + "'zs'}"));
        System.out.println(test);

    }
}
