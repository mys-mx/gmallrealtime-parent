package com.amos.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.amos.gmall.realtime.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @program: gmallrealtime-parent
 * @description: 准备用户行为日志DWD层
 * @create: 2021-08-16 21:28
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {
        // TODO 1.准备环境
        //  1.1创建flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  1.2 设置并行度（如何设置？ --> kafka分区数相等）
        env.setParallelism(3);
        //  1.3 设置CheckPoint
        // 每5000ms开启一次checkpoint 模式EXACTLY_ONCE(默认)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/gmall/flink/checkpoint/baselogApp"));


        //TODO 2. 从kafka中读取数据
        //  2.1调用kafka工具类，获取FlinkKafkaConsumer
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_group"));

        // TODO 3. 对读取到的数据格式进行转换  String-JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                return jsonObject;
            }
        });
//        jsonObjDS.print("json>>>>>>>");
        /* TODO 4.识别新老访客
            保存mid某天访问情况  (将首次访问日期作为状态保存起来), 等后面该设备再有日志过来，从状态中获取日期
            和日志产生日期进行对比，如果状态不为空，并且状态日期和当前日志日期不相等，说明是老访客，如果is_new标记是1，那么对其状态进行修复
        */
        // 4.1 根据mid对日志进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );
        // 4.2 新老访客进行修复  状态分为 算子状态和键控状态  我们这里几楼某一个设备的访问，使用键控状态比较合适
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 定义该mid访问状态
                    private ValueState<String> firstVisitDate;

                    // 定义日期格式化对像
                    private SimpleDateFormat sdf;

                    // 初始化操作
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //对状态和日期进行初始化
                        firstVisitDate = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDateState", String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取当前日志标记状态
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        //获取当前日志访问时间戳
                        Long ts = jsonObject.getLong("ts");
                        if ("1".equals(isNew)) {
                            //获取当前mid对应的状态
                            String stateDate = firstVisitDate.value();

                            // 对当前日志日期格式进行转换
                            String curDate = sdf.format(ts);
                            //如果状态不为空，并且状态日期和当前日志日期不相等
                            if (stateDate != null && stateDate.length() != 0) {
                                //是否为同一天数据
                                if (!stateDate.equals(curDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }

                            } else {
                                //如果还没记录设备状态，将当前访问日期为状态日期
                                firstVisitDate.update(curDate);
                            }


                        }
                        return jsonObject;
                    }
                }
        );

        jsonDSWithFlag.print(">>>>>>>>>>>>>>>>>>");


        env.execute();

    }


}
