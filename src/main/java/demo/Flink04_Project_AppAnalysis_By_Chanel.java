package demo;

import entity.MarketingUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author douglas
 * @create 2021-02-24 21:42
 */
public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new AppMarketingDataSource())
                .map(behavior-> Tuple2.of(behavior.getChannel()+"_"+behavior.getBehavior(),1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t->t.f0)
                .sum(1)
                .print();
        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior>{
        boolean canRun = true;
        Random random =new Random();
        List<String> channel=Arrays.asList("huawei","xiaomi","apple","baidu","qq","oppo","vivo");
        List<String> behaviors=Arrays.asList("download","install","update","uninstall");
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun){
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channel.get(random.nextInt(channel.size())),
                        System.currentTimeMillis()
                );
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun=false;
        }
    }
}
