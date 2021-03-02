package eventtimeAndWaterMark;

import entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author douglas
 * @create 2021-03-02 22:28
 */
public class Flink14_Chapter07_SplitAndSelect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> result = env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] datas = s.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .keyBy(ws -> ws.getTs())
                .process(new KeyedProcessFunction<Long, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        out.collect(value);
                        if (value.getVc() > 5) {
                            ctx.output(new OutputTag<WaterSensor>("警告") {
                            }, value);
                        }
                    }
                });
        result.print("主流");
        result.getSideOutput(new OutputTag<WaterSensor>("警告"){}).print("警告");
        env.execute();
    }
}
