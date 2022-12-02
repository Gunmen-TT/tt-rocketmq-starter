package cn.tt.rocketmq.demo;

import cn.tt.rocketmq.domain.BaseMqMessage;
import cn.tt.rocketmq.listener.BaseMqMessageListener;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

//@Component
//@RocketMQMessageListener(consumerGroup = "xxx", topic = "xxx")
public class RocketMqConsumer extends BaseMqMessageListener implements RocketMQListener<ConsumerEntityMessage> {


    @Override
    public void onMessage(ConsumerEntityMessage message) {
        //收到消息后先调用父类进行环绕处理
        super.messageAroundHandler(message);
    }


    @Override
    protected void businessHandler(BaseMqMessage message) {
        System.out.println("这里写业务处理...");
    }

    @Override
    protected boolean enableRetry() {
        return false;
    }

    @Override
    protected int maxReconsumeTimes() {
        //enableRetry 为 true 才会重试
        return 0;
    }
}
