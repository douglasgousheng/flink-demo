package Transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-02-18 21:12
 */
public class Flink02_TransForm_flatMap_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5)
                .flatMap((Integer value, Collector<Integer> out)->{
                    out.collect(value * value);
                    out .collect(value * value * value);
                }).returns(Types.INT)
                .print();
        env.execute();
    }
}
