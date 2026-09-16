package com.file_drop.constant;

import com.file_drop.entity.WebRTCMessage;
import org.springframework.web.socket.CloseStatus;

import java.util.Map;

public enum SignalingError {
  INVALID_PARAMETERS(4400, "Invalid room code or role"),
  ROOM_NOT_FOUND(4404, "Room not found"),
  ROOM_EXPIRED(4410, "Room expired"),
  ROLE_OCCUPIED(4409, "Role already occupied");

  private final int closeCode;
  private final String message;

  SignalingError(int closeCode, String message) {
    this.closeCode = closeCode;
    this.message = message;
  }

  public CloseStatus closeStatus() { return new CloseStatus(closeCode, name()); }

  public WebRTCMessage message() {
    return WebRTCMessage.builder().type(CommonConstant.ERROR)
        .payload(Map.of("code", name(), "message", message, "closeCode", closeCode)).build();
  }
}
