package com.oriole.wisepen.user.mq;

import com.oriole.wisepen.common.mq.ReliablePublisher;
import com.oriole.wisepen.user.api.domain.mq.UserTaskCompleteMessage;
import io.github.springwolf.core.asyncapi.annotations.AsyncMessage;
import io.github.springwolf.core.asyncapi.annotations.AsyncOperation;
import io.github.springwolf.core.asyncapi.annotations.AsyncPublisher;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static com.oriole.wisepen.user.api.constant.MqTopicConstants.TOPIC_USER_TASK_COMPLETE;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaUserEventPublisher {

    @Resource
    ReliablePublisher reliablePublisher;

    @AsyncPublisher(operation = @AsyncOperation(
            channelName = TOPIC_USER_TASK_COMPLETE,
            description = "发布用户任务完成请求，由用户服务异步写入任务记录并发放奖励。",
            payloadType = UserTaskCompleteMessage.class,
            message = @AsyncMessage(name = "UserTaskCompleteMessage", title = "用户任务完成请求")
    ))
    public void publishUserTaskComplete(UserTaskCompleteMessage message) {
        try {
            String userId = message.getUserId() == null ? null : String.valueOf(message.getUserId());
            reliablePublisher.publish(TOPIC_USER_TASK_COMPLETE, userId, message, UUID.randomUUID().toString());
            log.debug("user task complete publish requested. topic={} userId={} taskCode={}",
                    TOPIC_USER_TASK_COMPLETE, message.getUserId(), message.getTaskCode());
        } catch (Exception e) {
            log.error("user task complete publish request failed. topic={} userId={} taskCode={}",
                    TOPIC_USER_TASK_COMPLETE, message.getUserId(), message.getTaskCode(), e);
        }
    }
}
