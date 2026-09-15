package com.file_drop.handler;

import com.file_drop.service.WebRtcService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static org.mockito.Mockito.*;

class SignalingHandlerTest {
  @org.junit.jupiter.api.Test
  void pongIsPassedToLivenessCheck() throws Exception {
    var service = mock(WebRtcService.class);
    var session = mock(WebSocketSession.class);
    when(session.getAttributes()).thenReturn(java.util.Map.of("code", "abc123", "role", "sender"));
    var pong = new org.springframework.web.socket.PongMessage(java.nio.ByteBuffer.wrap(new byte[]{1, 2}));
    new SignalingHandler(service).handleMessage(session, pong);
    verify(service).receivePong("abc123", "sender", session, pong.getPayload());
  }

  @ParameterizedTest
  @ValueSource(strings = {"{", "null", "[]", "{}", "{\"type\":\"\"}",
      "{\"type\":\"joined\"}", "{\"type\":\"reset\"}"})
  void malformedAndServerReservedMessagesAreRejected(String payload) throws Exception {
    var service = mock(WebRtcService.class);
    var session = mock(WebSocketSession.class);
    new SignalingHandler(service).handleTextMessage(session, new TextMessage(payload));
    verify(session).close(CloseStatus.BAD_DATA);
    verifyNoInteractions(service);
  }
}
