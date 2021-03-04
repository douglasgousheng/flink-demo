package state;

import entity.WaterSensor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-03-04 22:14
 */
public class Flink02_State_Keyed_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        env.socketTextStream("hadoop102",9999)
                .map(value->{
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0],Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    private ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sumVcState=getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("sumVcState",Integer::sum,Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {
                        sumVcState.add(value.getVc());
                        out.collect(sumVcState.get());
                    }
                })
                .print();
        env.execute();
    }
}
