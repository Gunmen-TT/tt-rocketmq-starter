package cn.tt.rocketmq.domain;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * mq消息基类
 */

@Data
public abstract class BaseMqMessage {

    /**
     * 追踪id
     */
    private String tranceId = UUID.randomUUID().toString();

    /**
     * 发送来源标识
     */
    private String sources = "";

    /**
     * 发送时间
     */
    private LocalDateTime sendDateTime;

    /**
     * 重试次数，用于判断重试次数
     */
    private Integer retryTimes = 0;

    /**
     * 业务key标识
     */
    private String key;

}
