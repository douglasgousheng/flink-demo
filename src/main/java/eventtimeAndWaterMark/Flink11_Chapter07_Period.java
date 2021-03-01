package eventtimeAndWaterMark;

import entity.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author douglas
 * @create 2021-03-01 21:33
 */
public class Flink11_Chapter07_Period {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //封装为JavaBean
        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] datas = s.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });
        //创建水印生产策略
        WatermarkStrategy<WaterSensor> myWms = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                System.out.println("createWatermarkGenertor...");
                return new MyPeriod(3);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                System.out.println("recordTimestamp" + l);
                return waterSensor.getTs() * 1000;
            }
        });
        stream
                .assignTimestampsAndWatermarks(myWms)
                .keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg = "当前key："+key
                                +"窗口：["+context.window().getStart()/1000+","+
                                context.window().getEnd()/1000+")一共有 "
                                +elements.spliterator().estimateSize()+"条数据";
                        out.collect(context.window().toString());
                        out.collect(msg);
                    }
                })
                .print();
        env.execute();
    }

    public static class MyPeriod implements WatermarkGenerator<WaterSensor>{
        private long maxTs=Long.MIN_VALUE;
        //允许的最大延迟时间 ms
        private final long maxDelay;

        public MyPeriod(long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs=Long.MIN_VALUE+this.maxDelay+1;
        }

        //每收到一个元素，执行一次，用来生产waterMark中的时间戳
        @Override
        public void onEvent(WaterSensor waterSensor, long l, WatermarkOutput watermarkOutput) {
            System.out.println("onEvent..."+l);
            //有了新的元素找到最大的时间戳
            maxTs=Math.max(maxTs,l);
            System.out.println(maxTs);

        }

        //周期性的把WaterMark发射出去，默认周期是200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //周期性的发射水印：相当于Flink把自己的始终调慢了一个最大延迟
            output.emitWatermark(new Watermark(maxTs-maxDelay-1));

        }
    }
}
