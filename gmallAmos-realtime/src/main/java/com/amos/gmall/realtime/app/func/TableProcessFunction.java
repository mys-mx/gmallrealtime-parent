package com.amos.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.amos.gmall.realtime.bean.TableProcess;
import com.amos.gmall.realtime.common.GmallConfig;
import com.amos.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @program: gmallrealtime-parent
 * @description: 配置表处理函数, 做分流功能
 * @create: 2021-08-21 12:50
 */
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {


    private OutputTag<JSONObject> outputTag;
    // 用于在内存中存放配置表信息的Map <表名：操作，TableProcess>
    private Map<String, TableProcess> tableProcessMap=new HashMap<>();

    // 用于在内存中存放在phoenix已经建过的表
    private Set<String> existsTable = new HashSet<>();

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    Connection conn = null;

    // 在函数被调用的时候执行的方法，每个并行度执行一次
    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化phoenix链   接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 初始化配置表信息
        refreshMeta();



        //开启一个定时任务  周期性执行
        //因为配置表的数据可能会发生变化，需要 开启一个定时任务 从配置表中查询一次数据
        Timer timer = new Timer();
        // 从现在开始过delay 毫秒后，每隔 period执行一次
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 5000, 5000);
    }

    private void refreshMeta() {
        //从mysql数据配置表中查询配置信息
        System.out.println("查询配置表信息");
        List<TableProcess> tableProcesses = MySQLUtil.queryList(
                "select * from table_process",
                TableProcess.class,
                true);
        // 记录放到内存(Map)中
        for (TableProcess tableProcess :
                tableProcesses) {
            //获取源表的表名
            String sourceTable = tableProcess.getSourceTable();

            //获取操作类型
            String operateType = tableProcess.getOperateType();

            //输出目的地 表名或者主题名
            String sinkTable = tableProcess.getSinkTable();

            // 输出类型
            String sinkType = tableProcess.getSinkType();

            //输出字段
            String sinkColumns = tableProcess.getSinkColumns();

            //表的主键
            String sinkPk = tableProcess.getSinkPk();

            //建表的扩展语句
            String sinkExtend = tableProcess.getSinkExtend();

            String key = sourceTable + ":" + operateType;

            //将从配置表中的配置信息存到map集合中
            tableProcessMap.put(key, tableProcess);

            //=================3.如果当前配置项是维度配置，需要向hbase表中保存数据，那么我们需要判断phoenix中是否存在这张表=======================
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {

                boolean notExist = existsTable.add(sourceTable);
                //如果在内存set集合中不存在这个表，那么在phoenix中创建这张表
                if (notExist) {
                    //如果不存在我们要检查phoenix中是否存在这张表，有可能已经存在，
                    // 只不过是应用缓存被清空，导致当前这张表没有缓存，这种清空不需要创建表
                    // 在phoenix中，表地区不存在，那么需要将表创建起来

                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

                }
            }

            if (tableProcessMap == null || tableProcessMap.size() == 0) {
                throw new RuntimeException("没有从数据库的配置表中读到数据！！！！");
            }
        }
    }

    private void checkTable(String tableName, String fields, String pk, String ext) {

        //如果在配置表中，没有配置主键 需要一个默认的主键值

        if (pk == null) {
            pk = "id";
        }

        //如果在配置表中，没有配置建表扩展 需要一个默认的建表扩展的值
        if (ext == null) {
            ext = "";

        }

        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." + tableName + "(");


        String[] fieldsArr = fields.split(",");

        for (int i = 0; i < fieldsArr.length; i++) {

            //判断当前字段是否为主键
            if (fieldsArr[i].equals(pk)) {
                createSql.append(fieldsArr[i]).append(" varchar primary key ");
            } else {
                createSql.append("info.").append(fieldsArr[i]).append(" varchar ");
            }

            if (i < fieldsArr.length - 1)
                createSql.append(",");

        }
        createSql.append(")");
        createSql.append(ext);

        System.out.println("创建phoenix表的语句" + createSql.toString());
        // 需要链接phoenix
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException("phoenix 建表失败！！");
                }
            }
        }
    }


    //每过来一个元素，方法执行一次。主要任务是对当前进来的元素进行分流处理
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");

        String type = jsonObj.getString("type");

        //注意:问题修复，如果使用MaxWell的Bootstrap同步历史数据，这个时候她的操作类型叫做bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObj.put("type", type);
        }

        if (tableProcessMap != null && tableProcessMap.size() > 0) {


            //根据表名和操作类型拼接key
            String key = table + ":" + type;

            //要从内存的配置Map中获取当前key对象的配置信息
            TableProcess tableProcess = tableProcessMap.get(key);

            if (tableProcess != null) {
                jsonObj.put("sink_table", tableProcess.getSinkTable());
                String sinkColumns = tableProcess.getSinkColumns();
                //如果指定了sinkColumn，需要对保留的字段进行过滤处理
                if (sinkColumns != null && tableProcess.getSinkColumns().length() > 0)
                    filterColumn(jsonObj.getJSONObject("data"), sinkColumns);
            } else {
                System.out.println("No this key:" + key + " in Mysql");
            }
            //根据sinkType 将数据输出到不同的流
            if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                //如果sink type=hbase说明是维度数据，通过侧输出流输出
                ctx.output(outputTag, jsonObj);

            } else if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                //如果sink type=kafka说明是事实数据，通过主流输出

                out.collect(jsonObj);
            }

        }
    }

    //对data中数据进行过滤
    private void filterColumn(JSONObject data, String sinkColumns) {
        //sinkColumns 要保留那些列
        String[] cols = sinkColumns.split(",");
        List<String> colList = Arrays.asList(cols);


        // 获取json对象中封装的一个个键值对  每个键值对封装为Entry对象
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();


        //使用迭代器进行数组数据的删除
        for (; iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            if (!colList.contains(entry.getKey())) {
                iterator.remove();
            }
        }

    }
}
