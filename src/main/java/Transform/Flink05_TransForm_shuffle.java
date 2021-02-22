package Transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-02-18 22:27
 */
public class Flink05_TransForm_shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .shuffle()
                .print();
        env.execute();
    }
}
