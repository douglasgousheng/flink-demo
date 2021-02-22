package Transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author douglas
 * @create 2021-02-18 22:29
 */
public class Flink06_TransForm_connect {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从集合中读取数据
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        //从集合中读取数据
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");
        //把两个流连接在一起，貌合神离
        ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
        cs.getFirstInput().print("first");
        cs.getSecondInput().print("second");
        env.execute();

    }
}
