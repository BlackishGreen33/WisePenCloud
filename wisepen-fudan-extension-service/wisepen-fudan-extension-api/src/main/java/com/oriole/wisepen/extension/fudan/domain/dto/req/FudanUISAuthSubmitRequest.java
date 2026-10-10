package com.oriole.wisepen.extension.fudan.domain.dto.req;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FudanUISAuthSubmitRequest {

    @NotNull
    private Long userId;

    @NotBlank
    private String account;

    @NotBlank
    private String password;
}
