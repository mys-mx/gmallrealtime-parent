package com.amos.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.amos.gmall.realtime.app.func.DimSink;
import com.amos.gmall.realtime.app.func.TableProcessFunction;
import com.amos.gmall.realtime.bean.TableProcess;
import com.amos.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @program: gmallrealtime-parent
 * @description: 准备业务数据的DWD层
 * @create: 2021-08-21 00:13
 */
public class BaseDBApp {

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
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/gmall/flink/checkpoint/basedbApp"));
        env.setRestartStrategy(RestartStrategies.noRestart());


        //TODO 2. 从kafka中读取数据
        //  2.1调用kafka工具类，获取FlinkKafkaConsumer
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db_m", "base_db_app_group"));

        //TODO 3.对DS中数据进行结构转换  String--->Json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                line -> JSON.parseObject(line)
        );

        //TODO 4.对数据进行ETL  如果table为空 或者 data为空，或者长度小于3，将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                json -> {

                    boolean flag = json.getString("table") != null
                            && json.getJSONObject("data") != null
                            && json.getString("data").length() >= 3;
                    return flag;
                }
        );

//        filteredDS.print("json>>>>>>>>>>>>>");

        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };

        //TODO 5.动态分流 事实表放到主流，写回到kafka的DWD层；如果是维度表，通过侧输出流 写入到HBase中

        // 主流 写回到kafka的数据
        SingleOutputStreamOperator<JSONObject> mainDS = filteredDS.process(new TableProcessFunction(hbaseTag));

        //侧输出流 输出到Hbase
        DataStream<JSONObject> hbaseDS = mainDS.getSideOutput(hbaseTag);


        mainDS.print("事实>>>>>>>>>>>>");
        hbaseDS.print("维度>>>>>>>>>>>>");

        //TODO 6.将维度数据局保存到phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());

        //TODO 7.将事实数据协会到kafka的dwd层
        mainDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka 序列化！！！");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {

                        String sinkTopic = jsonObject.getString("sink_table");
                        JSONObject dataJsonObj = jsonObject.getJSONObject("data");

                        //可以在ProducerRecord里面传入key，泛型中的第一个参数为key 第二个为value
                        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(sinkTopic,dataJsonObj.toString().getBytes());
                        return producerRecord;
                    }
                }
        ));

        env.execute();

    }

}
