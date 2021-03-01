package window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author douglas
 * @create 2021-02-28 20:46
 */
public class Flink05_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Arrays.stream(s.split("\\W")).forEach(word->Tuple2.of(word,1L));
                    }
                })
                .keyBy(s->s.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    //创建累加器：初始化中间值
                    @Override
                    public Long createAccumulator() {
                        System.out.println("createAccumulator");
                        return 0L;
                    }

                    //累加器操作
                    @Override
                    public Long add(Tuple2<String, Long> value, Long aLong) {
                        System.out.println("add");
                        return aLong+value.f1;
                    }

                    //获取结果
                    @Override
                    public Long getResult(Long aLong) {
                        System.out.println("getResult");
                        return aLong;
                    }

                    //累加器的合并：只有会话窗口才会调用
                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("merge");
                        return a + b;
                    }
                })
                .print();
        env.execute();

    }
}
