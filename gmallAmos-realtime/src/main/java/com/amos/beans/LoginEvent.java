package com.amos.beans;

import lombok.Data;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2022-01-13 16:16
 */
@Data
public class LoginEvent {
    private Long userId;
    private String ip;

    private String loginState;
    private Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(Long userId, String ip, String loginState, Long timestamp) {
        this.userId = userId;
        this.ip = ip;
        this.loginState = loginState;
        this.timestamp = timestamp;
    }
}
