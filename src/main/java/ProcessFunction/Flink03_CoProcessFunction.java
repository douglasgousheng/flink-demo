package ProcessFunction;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-03-03 22:34
 */
public class Flink03_CoProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");

        ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
        cs
                .process(new CoProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);
                    }
                })
                .print();
        env.execute();


    }
}
