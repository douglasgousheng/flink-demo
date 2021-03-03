package ProcessFunction;

import entity.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-03-03 22:33
 */
public class Flink02_KeyedProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.getCurrentKey());
                        out.collect(value.toString());
                    }
                })
                .print();
    }
}
