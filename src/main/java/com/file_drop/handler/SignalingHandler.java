package com.file_drop.handler;

import com.file_drop.entity.WebRTCMessage;
import com.file_drop.service.WebRtcService;
import com.file_drop.util.JsonUtil;
import com.google.gson.JsonParseException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;

import static com.file_drop.constant.CommonConstant.*;

@Component
@RequiredArgsConstructor
public class SignalingHandler extends TextWebSocketHandler {
  private final WebRtcService webRtcService;

  @Override
  public void afterConnectionEstablished(WebSocketSession session) {
    webRtcService.addClient((String) session.getAttributes().get(ROLE), session,
        (String) session.getAttributes().get(CODE));
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
    webRtcService.removeClient((String) session.getAttributes().get(CODE),
        (String) session.getAttributes().get(ROLE), session);
  }

  @Override
  public void handleTransportError(WebSocketSession session, Throwable exception) throws IOException {
    afterConnectionClosed(session, CloseStatus.SERVER_ERROR);
    session.close(CloseStatus.SERVER_ERROR);
  }

  @Override
  protected void handlePongMessage(WebSocketSession session, PongMessage message) {
    webRtcService.receivePong((String) session.getAttributes().get(CODE),
        (String) session.getAttributes().get(ROLE), session, message.getPayload());
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
    WebRTCMessage msg;
    try {
      msg = JsonUtil.fromJson(message.getPayload(), WebRTCMessage.class);
    } catch (JsonParseException e) {
      session.close(CloseStatus.BAD_DATA);
      return;
    }
    if (msg == null || msg.getType() == null || msg.getType().isBlank()
        || JOINED.equals(msg.getType()) || RESET.equals(msg.getType())
        || ACCEPTED.equals(msg.getType()) || PEER_READY.equals(msg.getType()) || ERROR.equals(msg.getType())) {
      session.close(CloseStatus.BAD_DATA);
      return;
    }
    webRtcService.forwardMessage((String) session.getAttributes().get(CODE),
        (String) session.getAttributes().get(ROLE), session, msg);
  }
}
