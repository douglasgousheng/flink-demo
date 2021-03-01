package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author douglas
 * @create 2021-02-28 19:23
 */
public class Flink03_ProcessingTimeSessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Arrays.stream(s.split("\\W")).forEach(word->collector.collect(Tuple2.of(word,1L)));
                    }
                })
                .keyBy(t->t.f0)
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extract(Tuple2<String, Long> element) {
                        return element.f0.length() * 1000;
                    }
                }))
                .sum(1)
                .print();
        env.execute();
    }
}
