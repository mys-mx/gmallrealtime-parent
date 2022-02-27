package com.amos.cep;

import com.amos.beans.LoginEvent;
import com.amos.beans.LoginFailWarning;
import com.clearspring.analytics.util.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.List;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2022-01-13 16:08
 */
public class LoginFail {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");

        DataStreamSource<String> source = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<LoginEvent> loginEventStream = source.map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<LoginFailWarning> warningStreamProcess = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));
        warningStreamProcess.print();
        env.execute();
    }

    private static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        //最大连续登录失败次数
        private Integer MAX_FAILE_TIME;

        // 定义状态：保存2秒内所有的登录失败事件
        private ListState<LoginEvent> loginFailEventListState;
        // 定义状态：保存注册的定时器时间戳
        private ValueState<Long> timerTsState;

        public LoginFailDetectWarning(Integer MAX_FAILE_TIME) {
            this.MAX_FAILE_TIME = MAX_FAILE_TIME;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            //判断当前登录事件类型
            if ("fail".equals(value.getLoginState())) {
                //如果是失败事件 添加到列表状态中
                loginFailEventListState.add(value);
                //如果没有定时器 注册一个两秒之后的定时器
                if (timerTsState.value() == null) {
                    Long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                //如果登录成功，删除定时器，清空状态，重新开始
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {

            //定时器触发，说明2秒内没有登录成功来。判断liststate中失败的个数
            List<LoginEvent> loginfailEvents = Lists.newArrayList(loginFailEventListState.get());

            Integer failTimes = loginfailEvents.size();

            if (failTimes > MAX_FAILE_TIME) {
                //如果超出设定的最大失败次数输出报警
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginfailEvents.get(0).getTimestamp(),
                        loginfailEvents.get(failTimes - 1).getTimestamp(),
                        "Login fail  in 2s for " + failTimes + " times"
                ));
            }
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }
}
