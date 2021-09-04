package com.amos.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.amos.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2021-09-02 20:52
 */
public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/gmall/flink/checkpoint/uniquevisit"));


        //todo 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_group";
        String sinkTopic = "dwd_unique_visit";

        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        DataStreamSource<String> jsonStrDS = env.addSource(kafkaConsumer);

        // todo 3.对读取到的数据做结构化的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

//        jsonObjDS.print();

        //todo 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keybyMID = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));


        // todo 5.过滤得到uv
        SingleOutputStreamOperator<JSONObject> filterDS = keybyMID.filter(
                new RichFilterFunction<JSONObject>() {

                    //定义状态
                    ValueState<String> lastVisitDataState = null;


                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    //初始化状态以及日期工具类
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        sdf = new SimpleDateFormat("yyyyMMdd");

                        //获取状态描述器
                        ValueStateDescriptor<String> lastVisitDS = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //初始化状态

                        /*
                         * todo 构造者模式：
                         */
                        StateTtlConfig.Builder builder = StateTtlConfig.newBuilder(Time.days(1));
                        StateTtlConfig build = builder.build();

                        //因为我们统计的是日活，所以状态数据只在当天有效，过了一天就可以失效掉
                        lastVisitDS.enableTimeToLive(build);
                        this.lastVisitDataState = getRuntimeContext().getState(lastVisitDS);

                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("lat_page_id");

                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }

                        //获取当前访问时间
                        Long ts = jsonObject.getLong("ts");

                        String logDate = sdf.format(new Date(ts));
                        //当前页面的访问时间，和状态时间进行对比
                        String lastVisitDate = lastVisitDataState.value();

                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            System.out.println("已访问： lastVisitDate" + lastVisitDate + ",||logDate:" + logDate);
                            return false;
                        } else {
                            System.out.println("未访问：logDate:" + logDate);
                            lastVisitDataState.update(logDate);
                            return true;
                        }

                    }
                }
        );
        filterDS.print(">>>>>>>>>>>>>>>>>>>");

        //todo 6.向kafka中写回，需要将json转换为String

        SingleOutputStreamOperator<String> kafkaDS = filterDS.map(json -> json.toJSONString());


        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        env.execute();
    }

}
