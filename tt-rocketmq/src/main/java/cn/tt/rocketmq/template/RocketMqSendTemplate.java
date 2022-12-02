package cn.tt.rocketmq.template;

import cn.tt.rocketmq.domain.BaseMqMessage;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/***
 * rocketmq发送模板
 * @author TT
 */
@Component
@Slf4j
public class RocketMqSendTemplate {

    private static final String DELIMITER = ":";

    private static final DateTimeFormatter DF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 发送同步消息
     * @param topic
     * @param tag
     * @param message 消息体
     */
    public <T extends BaseMqMessage>SendResult syncSend(String topic, String tag, T message) {
        return this.syncSend(topic + DELIMITER + tag, message);
    }

    /**
     * 发送同步消息
     * @param destination topic/topic:tag
     * @param message 消息体
     * @return
     */
    public <T extends BaseMqMessage> SendResult syncSend(String destination, T message){
        message.setSendDateTime(LocalDateTime.now());
        Message<T> sendMessage = MessageBuilder.withPayload(message).setHeader(RocketMQHeaders.KEYS, message.getKey()).build();
        SendResult sendResult = rocketMQTemplate.syncSend(destination, sendMessage);
        rocketMQTemplate.convertAndSend("","");
        log.info("发送同步消息:[{}],发送结果[{}]", JSON.toJSONString(message),JSON.toJSONString(sendResult));
        return sendResult;
    }

    /**
     * 发送异步消息
     * @param topic
     * @param tag
     * @param message 消息体
     * @param sendCallback 发送消息回调处理
     */
    public <T extends BaseMqMessage> void asyncSend(String topic, String tag, T message, SendCallback sendCallback) {
        this.asyncSend(topic + DELIMITER + tag, message, sendCallback);
    }

    /**
     * 发送异步消息
     * @param topic
     * @param tag
     * @param message 消息体
     */
    public <T extends BaseMqMessage> void asyncSend(String topic, String tag, T message) {
        String topicTag = topic + DELIMITER + tag;
        this.asyncSend(topicTag, message, defaultSendCallback(topicTag, message));
    }

    /**
     * 发送异步消息
     * @param destination topic/topic:tag
     * @param message 消息体
     * @param <T>
     * @return
     */
    public <T extends BaseMqMessage> void asyncSend(String destination, T message) {
        this.asyncSend(destination, message, defaultSendCallback(destination, message));
    }

    /**
     * 发送异步消息
     * @param destination topic和tag的拼接
     * @param message 消息体
     * @param sendCallback 发送消息回调处理
     */
    public <T extends BaseMqMessage> void asyncSend(String destination, T message, SendCallback sendCallback) {
        message.setSendDateTime(LocalDateTime.now());
        Message<T> sendMessage = MessageBuilder.withPayload(message).setHeader(RocketMQHeaders.KEYS, message.getKey()).build();
        log.info("发送异步消息:[{}]", JSON.toJSONString(message));
        rocketMQTemplate.asyncSend(destination, sendMessage, sendCallback);
    }

    /**
     * 同步发送延迟消息
     * @param topic
     * @param tag
     * @param message 消息体
     * @param delayLevel 延迟等级
     */
    public <T extends BaseMqMessage> SendResult delaySyncSend(String topic, String tag, T message, int delayLevel) {
        return this.delaySyncSend(topic + DELIMITER + tag, message, delayLevel);
    }

    /**
     * 同步发送延迟消息
     * @param destination topic/topic:tag
     * @param message 消息体
     * @param delayLevel 延迟等级
     */
    public <T extends BaseMqMessage> SendResult delaySyncSend(String destination, T message, int delayLevel) {
        message.setSendDateTime(LocalDateTime.now());
        Message<T> sendMessage = MessageBuilder.withPayload(message).setHeader(RocketMQHeaders.KEYS, message.getKey()).build();
        SendResult sendResult = rocketMQTemplate.syncSend(destination, sendMessage, 3000, delayLevel);
        log.info("发送同步延迟消息:[{}],发送结果[{}]", JSON.toJSONString(message), JSON.toJSONString(sendResult));
        return sendResult;
    }

    /**
     * 异步发送延迟消息
     * @param topic
     * @param tag
     * @param message 消息体
     * @param delayLevel 延迟等级
     */
    public <T extends BaseMqMessage> void delayAsyncSend(String topic, String tag, T message, int delayLevel) {
        String topicTag = topic + DELIMITER + tag;
        this.delayAsyncSend(topicTag, message, defaultSendCallback(topicTag, message), delayLevel);
    }

    /**
     * 异步发送延迟消息
     * @param topic
     * @param tag
     * @param message 消息体
     * @param sendCallback 回调处理
     * @param delayLevel 延迟等级
     */
    public <T extends BaseMqMessage> void delayAsyncSend(String topic, String tag, T message, SendCallback sendCallback, int delayLevel) {
        this.delayAsyncSend(topic + DELIMITER + tag, message, sendCallback, delayLevel);
    }

    /**
     * 异步发送延迟消息
     * @param destination topic/topic:tag
     * @param message 消息体
     * @param delayLevel 延迟等级
     */
    public <T extends BaseMqMessage> void delayAsyncSend(String destination, T message, int delayLevel) {
        this.delayAsyncSend(destination, message, defaultSendCallback(destination, message), delayLevel);
    }

    /**
     * 异步发送延迟消息
     * @param destination topic/topic:tag
     * @param message 消息体
     * @param delayLevel 延迟等级
     */
    public <T extends BaseMqMessage> void delayAsyncSend(String destination, T message, SendCallback sendCallback, int delayLevel) {
        message.setSendDateTime(LocalDateTime.now());
        Message<T> sendMessage = MessageBuilder.withPayload(message).setHeader(RocketMQHeaders.KEYS, message.getKey()).build();
        log.info("发送延迟异步消息:[{}]", JSON.toJSONString(message));
        rocketMQTemplate.asyncSend(destination, sendMessage, sendCallback, 3000, delayLevel);
    }

    /**
     * 默认发送回调
     * @param destination topic和tag拼接值
     * @param message 消息体
     * @return 默认发送回调处理
     */
    private <T extends BaseMqMessage> SendCallback defaultSendCallback(String destination, T message) {
        return new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                if (SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                    log.info("发送消息成功:destination:[{}],tranceId:[{}],key[{}],sendDateTime:[{}],结果:[{}]",
                            destination, message.getTranceId(), message.getKey(), DF.format(message.getSendDateTime()), sendResult.toString());
                } else {
                    log.info("发送消息失败:destination:[{}],tranceId:[{}],key[{}],sendDateTime:[{}],结果:[{}]",
                            destination, message.getTranceId(), message.getKey(), DF.format(message.getSendDateTime()), sendResult.toString());
                }
            }

            @Override
            public void onException(Throwable throwable) {
                log.info("发送消息异常:destination:[{}],tranceId:[{}],key[{}],sendDateTime:[{}],异常消息:[{}]",
                        destination, message.getTranceId(), message.getKey(), DF.format(message.getSendDateTime()), throwable.getMessage());
            }
        };
    }

}
