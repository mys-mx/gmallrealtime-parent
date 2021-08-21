package com.amos.gmall.realtime.bean;

/**
 * @program: gmallrealtime-parent
 * @description: 配置表对应的实体类
 * @create: 2021-08-21 08:43
 * @Data: 自动创建 Getter, Setter, RequiredArgsConstructor, ToString, EqualsAndHashCode, Value
 */

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
//@Getter
//@Setter
public class TableProcess {

    //动态分流 Sink 常量 改为小写和脚本一致
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    private String sourceTable;
    //操作类型 insert,update,delete
    private String operateType;
    //输出类型 hbase kafka
    private String sinkType;
    //输出表(主题)
    private String sinkTable;
    //输出字段
    private String sinkColumns;

    //主键字段
    private String sinkPk;
    //建表扩展
    private String sinkExtend;

}
