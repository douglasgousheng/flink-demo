package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @author douglas
 * @create 2021-02-28 20:25
 */
public class Flink04_ReduceFunction {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从文件中读取数据
        env.socketTextStream("hadoop102",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Arrays.stream(s.split("\\W")).forEach(word->collector.collect(Tuple2.of(word,1L)));
                    }
                })
                .keyBy(s->s.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        System.out.println(value1+"------------"+value2);
                        //value1是上次聚合的结果，所以遇到每次创建的第一个元素时，这个函数不会进来
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                })
                .print();
        env.execute();
    }
}
