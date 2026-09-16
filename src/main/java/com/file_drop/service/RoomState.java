package com.file_drop.service;

import com.file_drop.entity.WebRTCRoom;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/** State and FIFO are guarded by this object's monitor; network I/O never is. */
final class RoomState extends WebRTCRoom {
  final ArrayDeque<Delivery> deliveries = new ArrayDeque<>();
  final java.util.Set<String> drainingSessions = new java.util.HashSet<>();
  final Map<String, Heartbeat> heartbeats = new HashMap<>();
  final byte[] senderTokenHash;

  RoomState(String code, byte[] senderTokenHash) {
    super(code);
    this.senderTokenHash = senderTokenHash.clone();
  }

  @Override
  public boolean equals(Object other) { return this == other; }

  @Override
  public int hashCode() { return System.identityHashCode(this); }

  static final class Heartbeat {
    long since;
    byte[] pending;

    Heartbeat(long now) { since = now; }
  }

  record Delivery(WebSocketSession session, String role, WebSocketMessage<?> message, CloseStatus closeStatus,
                  String expectedPeerId) {
    Delivery(WebSocketSession session, String role, WebSocketMessage<?> message, CloseStatus closeStatus) {
      this(session, role, message, closeStatus, null);
    }
  }
}
