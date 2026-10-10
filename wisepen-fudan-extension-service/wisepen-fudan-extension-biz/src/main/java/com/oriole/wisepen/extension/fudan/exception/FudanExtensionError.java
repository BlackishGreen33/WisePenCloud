package com.oriole.wisepen.extension.fudan.exception;

import com.oriole.wisepen.common.core.domain.IResult;
import com.oriole.wisepen.common.core.domain.ResultKey;
import com.oriole.wisepen.common.core.domain.enums.BusinessDomain;
import com.oriole.wisepen.common.core.exception.ErrorReason;
import com.oriole.wisepen.extension.fudan.constant.FudanExtensionSubject;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum FudanExtensionError implements IResult {

    UIS_TASK_NOT_FOUND(99111, new ResultKey(BusinessDomain.FUDAN_EXTENSION, FudanExtensionSubject.UIS, ErrorReason.NOT_FOUND), "任务不存在"),
    UIS_TASK_SUBMIT_FAILED(99112, new ResultKey(BusinessDomain.FUDAN_EXTENSION, FudanExtensionSubject.UIS, ErrorReason.FAILED), "UIS 认证任务提交失败");

    private final Integer code;
    private final ResultKey key;
    private final String msg;
}
