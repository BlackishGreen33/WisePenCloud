package com.oriole.wisepen.extension.fudan.cache;

import com.alibaba.nacos.common.utils.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oriole.wisepen.extension.fudan.domain.dto.FudanUISTaskResultDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class RedisCacheManager {


	private final StringRedisTemplate stringRedisTemplate;
	private final ObjectMapper objectMapper;

	private static final String REDIS_FUDAN_UIS_TASK_PREFIX = "wisepen:extension:fudan:uis:task:";
	private static final long TASK_EXPIRE_MINUTES = 30L;

	/**
	 * 设置 UIS 认证任务状态及结果 (30分钟有效)
	 */
	public void setUisTaskStatus(Long uid, FudanUISTaskResultDTO dto) {
		String key = REDIS_FUDAN_UIS_TASK_PREFIX + uid;
		try {
			String jsonValue = objectMapper.writeValueAsString(dto);
			stringRedisTemplate.opsForValue().set(key, jsonValue, TASK_EXPIRE_MINUTES, TimeUnit.MINUTES);
		} catch (JsonProcessingException ignored) {
		}
	}

	/**
	 * 获取 UIS 认证任务状态及结果
	 */
	public FudanUISTaskResultDTO getUisTaskStatus(Long uid) {
		String key = REDIS_FUDAN_UIS_TASK_PREFIX + uid;
		String jsonValue = stringRedisTemplate.opsForValue().get(key);
		if (!StringUtils.hasText(jsonValue)) {
			return null;
		}
		try {
			// 显式反序列化为指定类型，彻底杜绝 ClassCastException
			return objectMapper.readValue(jsonValue, FudanUISTaskResultDTO.class);
		} catch (JsonProcessingException e) {
			return null;
		}
	}

	/**
	 * 删除 UIS 认证任务缓存
	 */
	public void deleteUisTaskStatus(Long uid) {
		String key = REDIS_FUDAN_UIS_TASK_PREFIX + uid;
		stringRedisTemplate.delete(key);
	}

}
