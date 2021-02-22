package Transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-01-25 22:03
 * 匿名内部类对象
 * 需求：新的流元素是原来流的元素的平方
 */
public class Flink01_TransForm_Map_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> map1 = env.fromElements(1, 2, 3, 4, 5)
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }
                });
        map1.print();
        SingleOutputStreamOperator<Integer> map2 = env.fromElements(1, 2, 3, 4, 5)
                .map(ele -> ele * ele);
        map2.print();
        env.execute();
    }
}
