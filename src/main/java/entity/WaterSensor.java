package entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2021-01-24 20:42
 * 水位传感器：用于接收水位数据
 *
 * id：传感器编号
 * ts：时间戳
 * vc：水位
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
