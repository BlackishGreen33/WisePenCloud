package com.oriole.wisepen.extension.fudan.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oriole.wisepen.common.core.exception.ServiceException;
import com.oriole.wisepen.extension.fudan.cache.RedisCacheManager;
import com.oriole.wisepen.extension.fudan.domain.dto.FudanUISTaskResultDTO;
import com.oriole.wisepen.extension.fudan.domain.dto.req.FudanUISAuthSubmitRequest;
import com.oriole.wisepen.extension.fudan.domain.mq.FudanUISAuthRequestMessage;
import com.oriole.wisepen.extension.fudan.enums.FudanUISTaskState;
import com.oriole.wisepen.extension.fudan.exception.FudanExtensionError;
import io.github.springwolf.core.asyncapi.annotations.AsyncMessage;
import io.github.springwolf.core.asyncapi.annotations.AsyncOperation;
import io.github.springwolf.core.asyncapi.annotations.AsyncPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

import static com.oriole.wisepen.extension.fudan.constant.MqTopicConstants.FUDAN_UIS_AUTH_REQ;

@Slf4j
@Service
@RequiredArgsConstructor
public class UISAuthTaskService {

    private final RedisCacheManager redisCacheManager;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @AsyncPublisher(operation = @AsyncOperation(
            channelName = FUDAN_UIS_AUTH_REQ,
            description = "复旦扩展服务受理 UIS 认证任务后发布请求，由消费者驱动浏览器执行认证。",
            payloadType = FudanUISAuthRequestMessage.class,
            message = @AsyncMessage(name = "FudanUISAuthRequestMessage", title = "复旦 UIS 认证请求")
    ))
    public void submitTask(FudanUISAuthSubmitRequest request) throws JsonProcessingException {
        Long userId = request.getUserId();
        FudanUISAuthRequestMessage message = FudanUISAuthRequestMessage.builder()
                .userId(userId)
                .account(request.getAccount())
                .password(request.getPassword())
                .build();
        String payload = objectMapper.writeValueAsString(message);

        // 消息可能立即被消费，必须先建立可供前端轮询的状态。
        redisCacheManager.setUisTaskStatus(userId, FudanUISTaskResultDTO.of(FudanUISTaskState.QUEUED));
        try {
            // 受理成功以 Kafka 确认为准，不等待后续浏览器和扫码流程。
            kafkaTemplate.send(FUDAN_UIS_AUTH_REQ, payload).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            redisCacheManager.setUisTaskStatus(userId, FudanUISTaskResultDTO.of(FudanUISTaskState.FAILED_ERROR));
            ServiceException failure = new ServiceException(FudanExtensionError.UIS_TASK_SUBMIT_FAILED);
            failure.initCause(e);
            throw failure;
        }
        log.debug("uis auth request published. topic={} userId={}", FUDAN_UIS_AUTH_REQ, userId);
    }
}
