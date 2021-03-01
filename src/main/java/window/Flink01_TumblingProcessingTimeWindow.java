package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author douglas
 * @create 2021-02-28 19:05
 */
public class Flink01_TumblingProcessingTimeWindow {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从端口中读取数据
        env.socketTextStream("hadoop102",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split("\\W+")).forEach(word->out.collect(Tuple2.of(word,1L)));
                    }
                })
                .keyBy(t->t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(8)))//添加滚动窗口
        .sum(1)
                .print();
        env.execute();

    }
}
