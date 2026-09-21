package com.oriole.wisepen.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

/**
 * WisePen 安全相关配置。
 */
@Data
@Component
@RefreshScope
@ConfigurationProperties(prefix = "wisepen.security")
public class WisepenSecurityProperties {

    /**
     * 是否允许已登录但尚未完成身份认证的账号访问 @CheckRole 接口。
     */
    private boolean allowUnidentified;
}
