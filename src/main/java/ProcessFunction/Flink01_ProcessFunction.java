package ProcessFunction;

import entity.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-03-03 22:30
 */
public class Flink01_ProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("hadoop102",9999)
                .map(line->{
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0],Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
                })
                .process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }
                })
                .print();
        env.execute();
    }
}
