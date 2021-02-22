package Transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-02-18 21:05
 */
public class Flink02_TransForm_flatMap_Anonymous {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 新的流存储每个元素的平方和3次方
        env.fromElements(1,2,3,4,5).flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                out.collect(value * value);
                out.collect(value * value * value);
            }
        }).print();
        env.execute();
    }
}
