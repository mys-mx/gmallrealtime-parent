package com.amos.cep;

import com.amos.beans.LoginEvent;
import com.amos.beans.LoginFailWarning;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @program: gmallrealtime-parent
 * @description: cep编程
 * @create: 2022-01-13 18:52
 */
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中读取数据
        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");

        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<LoginEvent> loginEventStream = source.map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }
        )//
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getTimestamp() * 1000L;
                            }
                        }));

        /**
         * cep 编程步骤
         * 1. 定义一个匹配模式 pattern
         * 2. 将匹配模式应用到数据流上，得到一个pattern stream
         * 3. 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
         */
        // first fail-->second fail，within 2s

        // 开始登录失败
        Pattern<LoginEvent, LoginEvent> loginPattern = Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent elem) throws Exception {
                return "fail".equals(elem.getLoginState());
            }
        })
                //第二次登录失败
                .next("secondFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent elem) throws Exception {
                        return "fail".equals(elem.getLoginState());
                    }
                })      //在两秒范围内连续失败两次
                .within(Time.seconds(2));
        //将pattern应用到数据流中
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId),
                loginPattern);

        //检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> streamOperator = patternStream.select(new LoginFailMatchDetectWarning());

        streamOperator.print();
        env.execute("pattern execute");
    }

    private static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent firstFailEvent = pattern.get("firstFail").get(0);
            LoginEvent secondFailEvent = pattern.get("secondFail").get(0);
            System.out.println(pattern.keySet().toString());
            return new LoginFailWarning(firstFailEvent.getUserId(),
                    firstFailEvent.getTimestamp(),
                    secondFailEvent.getTimestamp(),
                    "log fail 2  times");
        }
    }
}
