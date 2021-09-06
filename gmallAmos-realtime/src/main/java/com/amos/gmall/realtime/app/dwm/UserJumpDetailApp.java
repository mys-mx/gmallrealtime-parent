package com.amos.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.amos.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2021-09-04 15:55
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/gmall/flink/checkpoint/userjumpdetail"));


        //todo 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";


        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        DataStreamSource<String> jsonStrDS = env.addSource(kafkaConsumer);

        // todo 3.对读取到的数据做结构化的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

//        jsonObjDS.print("json>>>>>>>>>>");
        //注意：从Flink1.12开始，默认的时间语义就是事件时间，不需要额外指定；
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //todo 4.指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }
                ));


        //todo 5.按照mid进行分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithTSDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        /*
         * 计算页面跳出明细 需要满足两个条件
         *   1.不是其他页面跳转过来的页面，是一个首次访问页面
         *               last_page_id == null
         *   2.距离首次访问结束后十秒内，没有对其他页面进行访问
         *
         * */
        //todo 6.配置mid进行分组
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(
                        //条件1：不是其他页面跳转过来的页面，是一个首次访问页面
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");

                                //判断是否为空 如果为空将为空的保留 费控的过滤掉
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                .next("next")
                .where(
                        //条件2：距离首次访问结束后十秒内，没有对其他页面进行访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取当前页面id
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                //判断当前访问的页面id是否为空
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                .within(Time.seconds(10));

        env.execute();

    }
}
