package cn.tt.rocketmq.listener;

import cn.tt.rocketmq.domain.BaseMqMessage;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/***
 * MQ抽象消息监听器
 * @author TT
 */

public abstract class BaseMqMessageListener <T extends BaseMqMessage>{

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * 业务处理
     * @param message 消息体
     */
    protected abstract void businessHandler(T message);

    /**
     * 是否开启重试
     * @return true:开启重试 false:关闭重试
     */
    protected abstract boolean enableRetry();

    /**
     * 最大重试次数(开启重试生效)
     * 若使用RocketMQ消费端默认重试机制最大重试次数16次,所以理应重试次数需<=16
     * @return 最大重试次数
     */
    protected abstract int maxReconsumeTimes();

    /**
     * 父类的切面处理(日志打印、重试处理等)
     * @param message 消息体
     */
    public void messageAroundHandler(T message) {
        log.info("消费者收到消息:[{}]", JSON.toJSONString(message));
        try {
            long startTime = System.currentTimeMillis();
            businessHandler(message);
            long endTime = System.currentTimeMillis();
            log.info("消息消费成功,sources[{}],tranceId[{}],耗时:[{}]ms", message.getSources(), message.getTranceId(), (endTime - startTime));
        } catch (Exception e) {
            //开启重试并且当前消息重试次数小于最大重试次数则抛出异常进行重试
            if (enableRetry() && message.getRetryTimes() < maxReconsumeTimes()) {
                log.error("消息消费异常,sources[{}],tranceId[{}]", message.getSources(), message.getTranceId());
                message.setRetryTimes(message.getRetryTimes() + 1);
                throw new RuntimeException(e);
            }
            log.error("消息消费异常,不重试,sources[{}],tranceId[{}]", message.getSources(), message.getTranceId(), e);
        }
    }

}
