package com.amos.gmall.realtime.utils;

/**
 * @program: gmallrealtime-parent
 * @description: 从 MySQL 读取数据的工具类
 * @create: 2021-08-21 08:48
 */


import com.amos.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLUtil {
    /**
     * mysql 查询方法，根据给定的 class 类型 返回对应类型的元素列表
     *
     * @param sql               执行的查询语句
     * @param clazz             返回的数据类型
     * @param underScoreToCamel 是否把对应字段的下划线名转为驼峰名
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //1.注册驱动 com.mysql.jdbc.Driver
            Class.forName("com.mysql.jdbc.Driver");
            //2.建立连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop01:3306/gmallAmos_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "muyusong521");

            //3.创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //4.执行 SQL 语句
            rs = ps.executeQuery();
            //5.处理结果集
            ResultSetMetaData md = rs.getMetaData();
            //声明集合对象，用于封装返回结果
            List<T> resultList = new ArrayList<T>();
            //每循环一次，获取一条查询结果
            while (rs.next()) {
                //通过反射创建要将查询结果转换为目标类型的对象
                T obj = clazz.getDeclaredConstructor().newInstance();
                //对查询出的列进行遍历，每遍历一次得到一个列名
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String propertyName = md.getColumnName(i);
                    //如果开启了下划线转驼峰的映射，那么将列名里的下划线转换为属性的打
                    if (underScoreToCamel) {
                        //直接调用 Google 的 guava 的 CaseFormat LOWER_UNDERSCORE 小写开头+下划线->LOWER_CAMEL 小写开头+驼峰
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,
                                propertyName);
                    }

                    //调用 apache 的 commons-bean 中的工具类，给 Bean 的属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                // 将当前结果中的一行数据封装到list集合中
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询 mysql 失败！");
        } finally {
            //6.释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 测试验证
     *
     * @param args
     */
    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process",
                TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
