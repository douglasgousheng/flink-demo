package Source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author douglas
 * @create 2021-01-25 21:17
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102,hadoop103,hadoop104");
        properties.setProperty("group.id","Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset","latest");
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new FlinkKafkaConsumer<>("sensor",new SimpleStringSchema(),properties))
                .print("kafka source");
        env.execute();
    }
}
