package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author douglas
 * @create 2021-02-28 20:56
 */
public class Flink06_ProcessWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Arrays.stream(s.split("\\W")).forEach(word -> collector.collect(Tuple2.of(word, 1L)));
                    }
                });
//                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
//                        System.out.println(context.window().getStart());
//                        long sum = 0L;
//                        for (Tuple2<String, Long> element : elements) {
//                            sum += element.f1;
//                        }
//                        out.collect(Tuple2.of(s,sum));
//                    }
//                })



    }
}
