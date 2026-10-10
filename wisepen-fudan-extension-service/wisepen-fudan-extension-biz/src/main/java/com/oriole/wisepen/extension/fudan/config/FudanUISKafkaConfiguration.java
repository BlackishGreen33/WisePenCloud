package com.oriole.wisepen.extension.fudan.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import static com.oriole.wisepen.extension.fudan.constant.MqTopicConstants.FUDAN_UIS_AUTH_REQ;

@Configuration
public class FudanUISKafkaConfiguration {

    @Bean
    public NewTopic fudanUisAuthRequestTopic() {
        // 配合 UIS 专属的 5 个消费线程；已有分区不足时由 KafkaAdmin 扩容，副本数沿用集群默认值。
        return TopicBuilder.name(FUDAN_UIS_AUTH_REQ).partitions(5).build();
    }
}
