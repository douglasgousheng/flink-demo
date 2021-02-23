package demo;

import entity.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-02-23 22:16
 */
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("input/UserBehavior.csv")
                .map(line->{
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])
                    );
                })
                .filter(behavior->"pv".equals(behavior.getBehavior()))
                .map(behavior-> Tuple2.of("pv",1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value->value.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
