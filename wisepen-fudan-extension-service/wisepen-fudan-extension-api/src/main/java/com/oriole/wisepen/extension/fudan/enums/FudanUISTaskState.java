package com.oriole.wisepen.extension.fudan.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum FudanUISTaskState {
    QUEUED(3, "QUEUED"), // 排队中
    PENDING(0, "PENDING"), // 处理中
    WAITING_SCAN(2, "WAITING_SCAN"), // 等待用户扫码
    SUCCESS(1, "SUCCESS"), // 成功
    FAILED_ERROR(-1, "FAILED_ERROR"),// 其他异常
    FAILED_AUTH(-2, "FAILED_AUTH"), // 账号密码错误或需进行其他验证
    FAILED_TIMEOUT(-3, "FAILED_TIMEOUT"); // 扫码超时

    private final int code;

    @EnumValue
    @JsonValue
    private final String value;
}
