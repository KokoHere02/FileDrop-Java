package com.file_drop.service;

import com.file_drop.constant.SignalingError;
import com.file_drop.entity.CreatedRoom;
import com.file_drop.util.SenderCredential;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.TextMessage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SenderAuthorizationTest {
  private final WebRtcService service = new WebRtcService();

  @AfterEach
  void shutdown() { service.shutdown(); }

  private WebSocketSession session(String id) {
    var session = mock(WebSocketSession.class);
    when(session.getId()).thenReturn(id);
    when(session.isOpen()).thenReturn(true);
    return session;
  }

  @Test
  void roomCodeAloneCannotClaimSenderAndReceiverDoesNotNeedCredential() throws Exception {
    CreatedRoom room = service.createRoom("file");
    var intruder = session("intruder");
    assertFalse(service.addClient("sender", intruder, room.code(), null));
    verify(intruder).close(SignalingError.SENDER_UNAUTHORIZED.closeStatus());
    service.removeClient(room.code(), "sender", intruder);
    assertTrue(service.addClient("receiver", session("receiver"), room.code(), null));
    var sender = session("sender");
    assertTrue(service.addClient("sender", sender, room.code(), room.senderToken()));
    service.removeClient(room.code(), "sender", sender);
    assertFalse(service.addClient("sender", session("intruder-again"), room.code(), null));
    assertTrue(service.addClient("sender", session("reconnected"), room.code(), room.senderToken()));
  }

  @Test
  void credentialIsBoundToRoomAndNeverEchoedInServerMessages() throws Exception {
    CreatedRoom first = service.createRoom("file");
    CreatedRoom second = service.createRoom("file");
    assertNotEquals(first.senderToken(), second.senderToken());
    var invalid = session("invalid");
    for (String token : new String[]{"", "wrong", first.code(), second.senderToken(), SenderCredential.generate()}) {
      assertFalse(service.addClient("sender", invalid, first.code(), token));
    }
    verify(invalid, times(5)).close(SignalingError.SENDER_UNAUTHORIZED.closeStatus());
    var sender = session("sender");
    var receiver = session("receiver");
    assertTrue(service.addClient("sender", sender, first.code(), first.senderToken()));
    assertTrue(service.addClient("receiver", receiver, first.code(), null));
    var capture = org.mockito.ArgumentCaptor.forClass(TextMessage.class);
    verify(receiver, times(2)).sendMessage(capture.capture());
    verify(sender, times(2)).sendMessage(capture.capture());
    for (TextMessage message : capture.getAllValues()) {
      assertFalse(message.getPayload().contains(first.senderToken()));
      assertFalse(message.getPayload().contains("senderToken"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void storesOnlyDigestAndUnauthorizedDisconnectCannotEvictCreator() throws Exception {
    CreatedRoom created = service.createRoom("file");
    Map<String, RoomState> rooms = (Map<String, RoomState>) ReflectionTestUtils.getField(service, "rooms");
    assertArrayEquals(SenderCredential.digest(created.senderToken()), rooms.get(created.code()).senderTokenHash);
    assertFalse(created.toString().contains(created.senderToken()));
    var sender = session("sender");
    var intruder = session("intruder");
    assertTrue(service.addClient("sender", sender, created.code(), created.senderToken()));
    assertFalse(service.addClient("sender", intruder, created.code(), null));
    service.removeClient(created.code(), "sender", intruder);
    var duplicate = session("duplicate");
    assertFalse(service.addClient("sender", duplicate, created.code(), created.senderToken()));
    verify(duplicate).close(SignalingError.ROLE_OCCUPIED.closeStatus());
  }
}
