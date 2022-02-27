package com.amos.gmall.realtime.common;

/**
 *
 * @author amos
 * @date 2022/02/27 17:24
 */
public class MysqlPropertiesConstants {

    /**
     * 反位 配置文件地址
     */
    public static final String MYSQL_PROPERTIES_FILE_NAME = "/mysql.properties";
    /**
     * mysql host
     */
    public static final String MYSQL_HOST = "mysql.host";
    /**
     * mysql 用户
     */
    public static final String MYSQL_USER = "mysql.user";
    /**
     * mysql 密码
     */
    public static final String MYSQL_PASSWORD = "mysql.password";
    /**
     * mysql 端口
     */
    public static final String MYSQL_PORT = "mysql.port";
    /**
     * mysql 数据库
     */
    public static final String MYSQL_DATABASES = "mysql.databases";

    /**
     * mysql 表
     */
    public static final String MYSQL_TABLES = "mysql.tables";

    /**
     * mysql 表
     */
    public static final String FLINK_CDC_BACKEND_PATH = "flink.cdc.backendPath";

    /**
     * 并行度
     */
    public static final String CDC_PARALLELISM="cdc.parallelism";
}
