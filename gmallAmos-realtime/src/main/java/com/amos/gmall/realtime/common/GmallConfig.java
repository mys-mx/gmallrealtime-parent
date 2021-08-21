package com.amos.gmall.realtime.common;

/**
 * @program: gmallrealtime-parent
 * @description: 项目配置的常量类
 * @create: 2021-08-21 16:14
 */
public class GmallConfig {
    //hbase的命名空间
    public static final String HBASE_SCHEMA = "GMALLAMOS_REALTIME";


    //phoenix链接地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181";


}
