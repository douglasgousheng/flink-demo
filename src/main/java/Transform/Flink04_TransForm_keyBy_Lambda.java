package Transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-02-18 22:22
 */
public class Flink04_TransForm_keyBy_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(10,3,5,9,20,8)
                .keyBy(value->value%2 == 0? "偶数":"奇数")
                .print();
        env.execute();
    }
}
