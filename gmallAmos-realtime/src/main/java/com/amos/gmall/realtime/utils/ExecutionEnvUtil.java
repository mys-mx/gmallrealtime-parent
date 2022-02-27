package com.amos.gmall.realtime.utils;


import com.amos.gmall.realtime.common.CommonPropertiesConstants;
import com.amos.gmall.realtime.common.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.concurrent.TimeUnit;

/**
 * env配置工具类
 * @author mys
 * @date 2022/02/27 17:04
 * @return
 */
public class ExecutionEnvUtil {

    public static ParameterTool createParameterToolNew(final String[] args, String propertiesPath) throws Exception {
        return ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(CommonPropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(propertiesPath)))
                .mergeWith(ParameterTool.fromArgs(args));
    }



    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 30分钟内 最大20次失败，每次间隔60秒
        env.getConfig().setRestartStrategy(RestartStrategies.failureRateRestart(20, Time.of(60, TimeUnit.MINUTES),
                Time.of(60, TimeUnit.SECONDS)));
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000));
        }
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 水位线更新频率
        env.getConfig().setAutoWatermarkInterval(1000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // checkpoint 过期时间 30分钟
        checkpointConfig.setCheckpointTimeout(1000 * 60 * 60);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }
}