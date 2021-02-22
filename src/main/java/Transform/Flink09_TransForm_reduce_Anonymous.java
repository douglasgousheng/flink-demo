package Transform;

import entity.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.bind.SchemaOutputResolver;
import java.util.ArrayList;

/**
 * @author douglas
 * @create 2021-02-19 21:15
 */
public class Flink09_TransForm_reduce_Anonymous {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建数组
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        //从数组中读取数据
        KeyedStream<WaterSensor, String> kbStream = env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);
        //匿名内部类执行reduce
        kbStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reducer function ...");
                return new WaterSensor(value1.getId(),value1.getTs(),value1.getVc()+value2.getVc());
            }
        })
                .print("reduce...");

        env.execute();
    }
}
