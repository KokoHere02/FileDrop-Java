package com.file_drop.entity;

import lombok.Data;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class WebRTCRoom implements Serializable {
  private String type;
  private String code;
  private WebRTCClient sender; // 发送者
  private WebRTCClient receiver;  // 接收者
  private LocalDateTime createdAt;
  private LocalDateTime expiresAt;

  public WebRTCRoom(String code) {
    this.code = code;
    this.createdAt = LocalDateTime.now();
    this.expiresAt = LocalDateTime.now().plusDays(1);
  }

  public WebRTCRoom(String code, LocalDateTime createdAt, LocalDateTime expiresAt) {
    this.code = code;
    this.createdAt = createdAt;
    this.expiresAt = expiresAt;
  }


}