package ProcessFunction;

import entity.WaterSensor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author douglas
 * @create 2021-03-03 22:37
 */
public class Flink03_ProcessJoinFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> s1 = env.socketTextStream("hadoop102", 8888)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });
        SingleOutputStreamOperator<WaterSensor> s2 = env
                .socketTextStream("hadoop102", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                });
        s1.join(s2)
                .where(WaterSensor::getId)
                .equalTo(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public String join(WaterSensor waterSensor, WaterSensor waterSensor2) throws Exception {
                        return "first:"+waterSensor+",second:"+waterSensor2;
                    }
                })
                .print();
        env.execute();
    }
}
