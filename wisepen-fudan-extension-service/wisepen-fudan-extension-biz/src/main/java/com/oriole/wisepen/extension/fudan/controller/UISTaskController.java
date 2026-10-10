package com.oriole.wisepen.extension.fudan.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.oriole.wisepen.common.core.domain.R;
import com.oriole.wisepen.common.core.exception.ServiceException;
import com.oriole.wisepen.extension.fudan.cache.RedisCacheManager;
import com.oriole.wisepen.extension.fudan.domain.dto.FudanUISTaskResultDTO;
import com.oriole.wisepen.extension.fudan.domain.dto.req.FudanUISAuthSubmitRequest;
import com.oriole.wisepen.extension.fudan.exception.FudanExtensionError;
import com.oriole.wisepen.extension.fudan.service.UISAuthTaskService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@Tag(name = "内部 - 复旦 UIS 认证", description = "供用户服务提交复旦 UIS 认证任务及查询状态")
@RestController
@RequestMapping("/internal/extenion/fudan/uis")
@RequiredArgsConstructor
public class UISTaskController {

    private final RedisCacheManager redisCacheManager;
    private final UISAuthTaskService uisAuthTaskService;

    @Operation(
            summary = "内部提交复旦 UIS 认证任务",
            description = """
                    - 用途：供用户服务提交认证任务，由复旦扩展服务管理任务排队和执行。
                    - 请求：userId 指定用户，account 和 password 为 UIS 凭据。
                    - 约束：调用方必须通过内部服务调用边界，并已校验用户可以发起认证。
                    - 处理：先写入有效期 30 分钟的 QUEUED 状态，再发布 Kafka 消息并等待发送确认；消费者开始执行时更新为 PENDING。
                    - 失败：消息发布失败或等待确认超时 -> FudanExtensionError.UIS_TASK_SUBMIT_FAILED；缓存写入或序列化发生未处理异常 -> CommonError.INTERNAL_ERROR。
                    - 响应：成功时返回空结果，表示任务已受理，最终认证结果需查询任务状态。
                    """
    )
    @PostMapping("/submitUISVerificationTask")
    public R<Void> submitTask(@Valid @RequestBody FudanUISAuthSubmitRequest request) throws JsonProcessingException {
        uisAuthTaskService.submitTask(request);
        return R.ok();
    }

    @Operation(
            summary = "内部获取复旦 UIS 认证状态",
            description = """
                    - 用途：供用户服务查询指定用户的复旦 UIS 身份认证任务结果。
                    - 请求：userId 指定发起 UIS 认证的用户。
                    - 约束：调用方必须通过内部服务调用边界；任务状态必须仍存在于缓存中。
                    - 处理：从 Redis 读取用户 UIS 任务结果；不主动发起新的 UIS 认证，也不修改用户认证状态。
                    - 失败：任务不存在或已过期 -> FudanExtensionError.UIS_TASK_NOT_FOUND；缓存读取发生未处理异常 -> CommonError.INTERNAL_ERROR。
                    - 响应：返回 UIS 任务结果和状态信息。
                    """
    )
    @GetMapping("/getUISVerificationStatus")
    public R<FudanUISTaskResultDTO> getTaskStatus(@RequestParam Long userId) {
        FudanUISTaskResultDTO result = redisCacheManager.getUisTaskStatus(userId);

        if (result == null) {
            throw new ServiceException(FudanExtensionError.UIS_TASK_NOT_FOUND);
        }
        return R.ok(result);
    }
}
