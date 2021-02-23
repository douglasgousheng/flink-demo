package Sink;


import com.alibaba.fastjson.JSON;
import entity.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @author douglas
 * @create 2021-02-22 21:53
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        //连接redis的环境配置
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从集合中读取，并写入到redis中
        env
                .fromCollection(waterSensors)
                .addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
                    /*
                        key                 value(hash)
                        "sensor"            field           value
                        sensor_1        {"id":"sensor_1","ts":1607527992000,"vc":20}
                        ...             ...
                    */
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        //返回存在Redis中的数据类型，存储的是Hash，第二个参数是外面的key
                        return new RedisCommandDescription(RedisCommand.HSET,"sensor");
                    }

                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        //从数据中获取value:Hash的Key
                        return data.getId();
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        //从数据中获取value:Hash的Value
                        return JSON.toJSONString(data);
                    }
                }));

        env.execute();
    }
}
