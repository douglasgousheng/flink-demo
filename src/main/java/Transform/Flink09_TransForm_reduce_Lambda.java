package Transform;

import entity.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author douglas
 * @create 2021-02-19 21:20
 */
public class Flink09_TransForm_reduce_Lambda {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        //按照id进行分区
        KeyedStream<WaterSensor, String> kbStream = env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);
        //lambda表达式执行reduce
        kbStream
                .reduce((value1,value2)->{
                    System.out.println("reducer function ...");
                    return new WaterSensor(value1.getId(),value1.getTs(),value1.getVc()+value2.getVc());
                })
                .print("recuder");
        env.execute();

    }
}
