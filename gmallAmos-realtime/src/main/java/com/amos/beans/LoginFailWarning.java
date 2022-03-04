package com.amos.beans;

import lombok.Data;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2022-01-13 16:19
 */
@Data
public class LoginFailWarning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;

    private String warningMag;

    public LoginFailWarning() {
    }

    public LoginFailWarning(Long userId, Long firstFailTime, Long lastFailTime, String warningMag) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMag = warningMag;
    }

}
