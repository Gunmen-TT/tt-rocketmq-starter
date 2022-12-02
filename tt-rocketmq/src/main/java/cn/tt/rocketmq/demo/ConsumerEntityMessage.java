package cn.tt.rocketmq.demo;

import cn.tt.rocketmq.domain.BaseMqMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ConsumerEntityMessage extends BaseMqMessage {

    private String data;
}
