package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author douglas
 * @create 2021-02-28 19:15
 */
public class Flink02_SlidingProcessingTimeWindows {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从文件中读取数据创建流，并求总数
        env.socketTextStream("hadoop102",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Arrays.stream(s.split("\\W")).forEach(word->collector.collect(Tuple2.of(word,1L)));
                    }
                })
                .keyBy(t->t.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))//添加滑动窗口
        .sum(1)
                .print();
        env.execute();

    }
}
