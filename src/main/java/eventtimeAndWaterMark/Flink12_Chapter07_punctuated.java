package eventtimeAndWaterMark;

import entity.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author douglas
 * @create 2021-03-02 21:03
 */
public class Flink12_Chapter07_punctuated {
    public static void main(String[] args) {

    }

    public static class MyPunctuated implements WatermarkGenerator<WaterSensor> {
        private long maxTs;
        //语序最大的延迟时间ms
        private final long maxDelay;

        public MyPunctuated(long maxDelay) {
            this.maxDelay = maxDelay * 1000;
            this.maxTs=Long.MIN_VALUE+this.maxDelay+1;
        }


        //每收到一个元素，执行一次，用来生产WaterMark中的时间戳
        @Override
        public void onEvent(WaterSensor waterSensor, long l, WatermarkOutput output) {
            System.out.println("onEvent..."+l);
            //有了新的元素找到最大的时间戳
            maxTs=Math.max(maxTs,l);
            output.emitWatermark(new Watermark(maxTs-maxDelay-1));

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

        }
    }
}
