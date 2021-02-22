package Transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-02-18 21:28
 */
public class Flink03_TransForm_filter_Lambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(10, 3, 5, 9, 20, 8)
                .filter(value->value%2==0)
                .print();
        env.execute();
    }
}
