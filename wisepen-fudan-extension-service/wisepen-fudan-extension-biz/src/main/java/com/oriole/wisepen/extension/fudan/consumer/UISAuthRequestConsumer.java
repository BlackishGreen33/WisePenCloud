package com.oriole.wisepen.extension.fudan.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.*;
import com.microsoft.playwright.options.LoadState;
import com.oriole.wisepen.extension.fudan.constant.MqTopicConstants;
import com.oriole.wisepen.extension.fudan.domain.dto.FudanUISTaskResultDTO;
import com.oriole.wisepen.extension.fudan.domain.mq.FudanUISAuthRequestMessage;
import com.oriole.wisepen.extension.fudan.enums.FudanUISTaskState;
import io.github.springwolf.core.asyncapi.annotations.AsyncListener;
import io.github.springwolf.core.asyncapi.annotations.AsyncMessage;
import io.github.springwolf.core.asyncapi.annotations.AsyncOperation;
import io.github.springwolf.plugins.kafka.asyncapi.annotations.KafkaAsyncOperationBinding;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oriole.wisepen.extension.fudan.cache.RedisCacheManager;

@Slf4j
@Component
@RequiredArgsConstructor
public class UISAuthRequestConsumer {

    private final RedisCacheManager redisCacheManager;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = MqTopicConstants.FUDAN_UIS_AUTH_REQ,
            groupId = "wisepen-extension-fudan-uis-auth-group", concurrency = "5", properties = "max.poll.records=1")
    @AsyncListener(operation = @AsyncOperation(
            channelName = MqTopicConstants.FUDAN_UIS_AUTH_REQ,
            description = "消费复旦 UIS 认证请求，驱动浏览器完成 UIS 登录流程并把任务状态写入 Redis。",
            payloadType = FudanUISAuthRequestMessage.class,
            message = @AsyncMessage(name = "FudanUISAuthRequestMessage", title = "复旦 UIS 认证请求")
    ))
    @KafkaAsyncOperationBinding(groupId = "wisepen-extension-fudan-uis-auth-group")
    public void handleUisAuthRequest(String payload) throws Exception {
        FudanUISAuthRequestMessage msg = objectMapper.readValue(payload, FudanUISAuthRequestMessage.class);
        log.info("uis auth request received. topic={} userId={} account={}",
                MqTopicConstants.FUDAN_UIS_AUTH_REQ, msg.getUserId(), msg.getAccount());
        runScraper(msg.getUserId(), msg.getAccount(), msg.getPassword());
        log.debug("uis auth request consumed. topic={} userId={}",
                MqTopicConstants.FUDAN_UIS_AUTH_REQ, msg.getUserId());
    }

    private void runScraper(Long uid, String account, String password) {
        redisCacheManager.setUisTaskStatus(uid, FudanUISTaskResultDTO.of(FudanUISTaskState.PENDING));
        log.info("uis auth task changed. userId={} from=QUEUED to=PENDING", uid);

        try (Playwright playwright = Playwright.create()) {
            Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions()
                    .setHeadless(true).setArgs(Arrays.asList("--no-sandbox", "--disable-dev-shm-usage")));
            BrowserContext context = browser.newContext();
            Page page = context.newPage();

            page.navigate("https://my.fudan.edu.cn/");
            page.locator("#login-username").fill(account);
            page.locator("#login-password").fill(password);

            Response qrResponse = page.waitForResponse(
                    response -> response.url().contains("getAuthQr") && response.status() == 200,
                    () -> page.locator(".content_submit").click()
            );

            String base64Image = extractQrBase64(qrResponse.text());
            if (base64Image == null) {
                redisCacheManager.setUisTaskStatus(uid, FudanUISTaskResultDTO.of(FudanUISTaskState.FAILED_AUTH));
                log.warn("uis auth task changed. userId={} from=PENDING to=FAILED_AUTH reason=\"qr code missing\"", uid);
                return;
            }

            FudanUISTaskResultDTO waitingDto = FudanUISTaskResultDTO.of(FudanUISTaskState.WAITING_SCAN);
            waitingDto.setQrBase64(base64Image);
            redisCacheManager.setUisTaskStatus(uid, waitingDto);
            log.debug("uis auth task changed. userId={} from=PENDING to=WAITING_SCAN", uid);

            try {
                page.waitForURL(Pattern.compile(".*my\\.fudan\\.edu\\.cn.*"),
                        new Page.WaitForURLOptions().setTimeout(120_000));

                page.waitForLoadState(LoadState.NETWORKIDLE);
                Map<String, String> infoMap = parseStudentInfo(page);
                infoMap.put("学号", account);

                FudanUISTaskResultDTO successDto = FudanUISTaskResultDTO.of(FudanUISTaskState.SUCCESS);
                successDto.setProfile(infoMap);
                redisCacheManager.setUisTaskStatus(uid, successDto);
                log.info("uis auth task changed. userId={} from=WAITING_SCAN to=SUCCESS", uid);

            } catch (TimeoutError e) {
                redisCacheManager.setUisTaskStatus(uid, FudanUISTaskResultDTO.of(FudanUISTaskState.FAILED_TIMEOUT));
                log.error("uis auth task changed. userId={} from=WAITING_SCAN to=FAILED_TIMEOUT", uid, e);
            }

        } catch (Exception e) {
            redisCacheManager.setUisTaskStatus(uid, FudanUISTaskResultDTO.of(FudanUISTaskState.FAILED_ERROR));
            log.error("uis auth task changed. userId={} to=FAILED_ERROR", uid, e);
        }
    }

    /**
     * 利用正则提取 qrMsg 中的纯 Base64 图片数据
     */
    private String extractQrBase64(String json) {
        Pattern p = Pattern.compile("\"qrMsg\"\\s*:\\s*\"data:image/png;base64,([^\"]+)\"");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    /**
     * 解析登录成功后页面上的学生信息表格
     */
    private Map<String, String> parseStudentInfo(Page page) {
        Map<String, String> infoMap = new LinkedHashMap<>();

        // 定位表格中的所有单元格
        // 选择器策略：找到 .card-body 下的 .table 里的所有 td
        Locator cells = page.locator(".card-body .table td");
        int count = cells.count();

        // 步长为 2，因为数据是 Key-Value 成对出现的
        for (int i = 0; i < count; i += 2) {
            // 提取 Key (移除冒号和多余空白)
            String key = cells.nth(i).innerText().trim().replace("：", "");
            // 提取 Value
            String value = cells.nth(i + 1).innerText().trim();
            infoMap.put(key, value);
        }

        return infoMap;
    }
}
