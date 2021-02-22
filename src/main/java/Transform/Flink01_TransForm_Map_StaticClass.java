package Transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-01-25 22:07
 * 静态内部类对象
 * 需求：新的流元素是原来流的元素的平方
 */
public class Flink01_TransForm_Map_StaticClass {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5,6)
                .map(new MyMapFunction())
                .print();
        env.execute();
    }
    public static class MyMapFunction implements MapFunction<Integer,Integer>{

        @Override
        public Integer map(Integer value) throws Exception {
            return value*value;
        }
    }
}
