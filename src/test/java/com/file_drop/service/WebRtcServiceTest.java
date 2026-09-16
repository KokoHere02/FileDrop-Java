package com.file_drop.service;

import com.file_drop.entity.WebRTCMessage;
import com.file_drop.entity.WebRTCRoom;
import com.file_drop.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebRtcServiceTest {
  private final WebRtcService service = new WebRtcService();

  private final java.util.Map<String, String> senderTokens = new java.util.concurrent.ConcurrentHashMap<>();

  private String createRoom() {
    var created = service.createRoom("file");
    senderTokens.put(created.code(), created.senderToken());
    return created.code();
  }

  private final String code = createRoom();

  private boolean join(String role, WebSocketSession session, String code) {
    return service.addClient(role, session, code, code == null ? null : senderTokens.get(code));
  }


  @AfterEach
  void shutdown() throws InterruptedException {
    ThreadPoolExecutor cleanup = (ThreadPoolExecutor) ReflectionTestUtils.getField(service, "cleanupExecutor");
    cleanup.shutdown();
    try {
      assertTrue(cleanup.awaitTermination(3, TimeUnit.SECONDS), "Cleanup worker did not finish");
    } finally {
      service.shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, WebRTCRoom> rooms() {
    return (Map<String, WebRTCRoom>) ReflectionTestUtils.getField(service, "rooms");
  }

  private WebSocketSession session(String id) {
    WebSocketSession session = mock(WebSocketSession.class);
    when(session.getId()).thenReturn(id);
    when(session.isOpen()).thenReturn(true);
    return session;
  }

  @Test
  void loneClientCanDisconnectAndReconnect() {
    WebSocketSession first = session("first");
    assertTrue(join("sender", first, code));
    service.removeClient(code, "sender", first);
    assertTrue(join("sender", session("replacement"), code));
  }

  @Test
  void failedAdmissionConfirmationDoesNotPublishFalseReadiness() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    join("sender", sender, code);
    clearInvocations(sender);
    doThrow(new IOException("confirmation failed")).when(receiver).sendMessage(any());
    join("receiver", receiver, code);
    var capture = org.mockito.ArgumentCaptor.forClass(TextMessage.class);
    verify(sender).sendMessage(capture.capture());
    assertEquals("reset", JsonUtil.fromJson(capture.getValue().getPayload(), WebRTCMessage.class).getType());
    assertTrue(join("receiver", session("replacement"), code));
  }

  @Test
  void rejectedAdmissionStillClosesWhenErrorMessageCannotBeSent() throws Exception {
    WebSocketSession invalid = session("invalid");
    doThrow(new IOException("socket unavailable")).when(invalid).sendMessage(any());
    assertFalse(join("sender", invalid, "absent"));
    verify(invalid).close(com.file_drop.constant.SignalingError.ROOM_NOT_FOUND.closeStatus());
  }

  @Test
  void rejectedConnectionCannotEvictOrImpersonateOccupant() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    WebSocketSession intruder = session("intruder");
    join("sender", sender, code);
    join("receiver", receiver, code);
    clearInvocations(sender, receiver);
    assertFalse(join("sender", intruder, code));
    service.removeClient(code, "sender", intruder);
    service.forwardMessage(code, "sender", intruder, WebRTCMessage.builder().type("offer").build());
    verify(receiver, never()).sendMessage(any());
    assertFalse(join("sender", session("another"), code));
    service.forwardMessage(code, "sender", sender, WebRTCMessage.builder().type("offer").build());
    verify(receiver).sendMessage(any(TextMessage.class));
  }

  @Test
  void joinedAndForwardedMessagesHaveDistinctServerOwnedIds() throws Exception {
    for (String firstRole : new String[]{"sender", "receiver"}) {
      String roomCode = createRoom();
      String secondRole = firstRole.equals("sender") ? "receiver" : "sender";
      WebSocketSession first = session("first-" + firstRole);
      WebSocketSession second = session("second-" + firstRole);
      join(firstRole, first, roomCode);
      join(secondRole, second, roomCode);
      var capture = org.mockito.ArgumentCaptor.forClass(TextMessage.class);
      verify(first, times(2)).sendMessage(capture.capture());
      WebRTCMessage joined = JsonUtil.fromJson(capture.getValue().getPayload(), WebRTCMessage.class);
      assertEquals("peer-ready", joined.getType());
      clearInvocations(second);
      assertNotEquals(joined.getFrom(), joined.getTo());
      service.forwardMessage(roomCode, firstRole, first,
          WebRTCMessage.builder().type("offer").from("spoofed").to("spoofed").payload("sdp").build());
      verify(second).sendMessage(capture.capture());
      WebRTCMessage forwarded = JsonUtil.fromJson(capture.getValue().getPayload(), WebRTCMessage.class);
      assertEquals(joined.getTo(), forwarded.getFrom());
      assertEquals(joined.getFrom(), forwarded.getTo());
      assertEquals("sdp", forwarded.getPayload());
    }
  }

  @Test
  void failedResetNotificationDoesNotLeaveOccupiedSlots() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    join("sender", sender, code);
    join("receiver", receiver, code);
    clearInvocations(sender, receiver);
    doThrow(new IOException("disconnected")).when(sender).sendMessage(any());
    service.removeClient(code, "receiver", receiver);
    assertTrue(join("receiver", session("new-receiver"), code));
    assertTrue(join("sender", session("new-sender"), code));
  }

  @Test
  void simultaneousConnectionsCannotClaimSameRole() throws Exception {
    var executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      var first = executor.submit(() -> { start.await(); return join("sender", session("a"), code); });
      var second = executor.submit(() -> { start.await(); return join("sender", session("b"), code); });
      start.countDown();
      assertNotEquals(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void expiredRoomRejectsConnectionsAndClosesExistingSessions() throws Exception {
    WebSocketSession sender = session("sender");
    join("sender", sender, code);
    Map<String, WebRTCRoom> rooms = (Map<String, WebRTCRoom>) ReflectionTestUtils.getField(service, "rooms");
    rooms.get(code).setExpiresAt(LocalDateTime.now().minusSeconds(1));
    assertFalse(join("receiver", session("receiver"), code));
    service.removeExpiredRooms();
    assertFalse(rooms.containsKey(code));
    verify(sender, timeout(2000)).close(com.file_drop.constant.SignalingError.ROOM_EXPIRED.closeStatus());
  }

  @Test
  void invalidRoomAndRoleAreRejected() throws Exception {
    WebSocketSession invalid = session("invalid");
    assertFalse(join(null, invalid, null));
    assertFalse(join("unknown", invalid, code));
    assertFalse(join("sender", invalid, "absent"));
    verify(invalid, times(2)).close(com.file_drop.constant.SignalingError.INVALID_PARAMETERS.closeStatus());
    verify(invalid).close(com.file_drop.constant.SignalingError.ROOM_NOT_FOUND.closeStatus());
    assertThrows(ResponseStatusException.class, () -> service.createRoom(" "));
  }

  @Test
  void slowSendDoesNotBlockMembershipChangesAndPreservesResetBeforeJoined() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    join("sender", sender, code);
    join("receiver", receiver, code);
    clearInvocations(sender, receiver);
    WebRTCRoom room = rooms().get(code);
    CountDownLatch sending = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    List<String> messages = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      assertFalse(Thread.holdsLock(room), "Socket send must not own the room monitor");
      TextMessage message = invocation.getArgument(0);
      String type = JsonUtil.fromJson(message.getPayload(), WebRTCMessage.class).getType();
      messages.add(type);
      if (type.equals("candidate")) {
        sending.countDown();
        assertTrue(release.await(5, TimeUnit.SECONDS));
      }
      return null;
    }).when(sender).sendMessage(any());
    var executor = Executors.newFixedThreadPool(2);
    try {
      var blocked = executor.submit(() -> service.forwardMessage(code, "receiver", receiver,
          WebRTCMessage.builder().type("candidate").build()));
      assertTrue(sending.await(2, TimeUnit.SECONDS));
      var membership = executor.submit(() -> {
        service.removeClient(code, "receiver", receiver);
        return join("receiver", session("replacement"), code);
      });
      assertTrue(membership.get(2, TimeUnit.SECONDS));
      assertEquals(List.of("candidate"), messages);
      release.countDown();
      blocked.get(2, TimeUnit.SECONDS);
      assertEquals(List.of("candidate", "reset", "peer-ready"), messages);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void queuedMessagesAreNotDeliveredToReplacedSession() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    WebSocketSession replacement = session("replacement");
    join("sender", sender, code);
    join("receiver", receiver, code);
    clearInvocations(sender, receiver);
    CountDownLatch sending = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      sending.countDown();
      assertTrue(release.await(5, TimeUnit.SECONDS));
      return null;
    }).when(receiver).sendMessage(any());
    var executor = Executors.newSingleThreadExecutor();
    try {
      var blocked = executor.submit(() -> service.forwardMessage(code, "sender", sender,
          WebRTCMessage.builder().type("offer").build()));
      assertTrue(sending.await(2, TimeUnit.SECONDS));
      service.forwardMessage(code, "sender", sender, WebRTCMessage.builder().type("candidate").build());
      service.removeClient(code, "receiver", receiver);
      assertTrue(join("receiver", replacement, code));
      release.countDown();
      blocked.get(2, TimeUnit.SECONDS);
      verify(receiver, times(1)).sendMessage(any());
      verify(replacement, times(2)).sendMessage(any());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void blockedCloseDoesNotBlockExpirySweepsOrHoldRoomLock() throws Exception {
    WebSocketSession sender = session("sender");
    join("sender", sender, code);
    WebRTCRoom room = rooms().get(code);
    room.setExpiresAt(LocalDateTime.now().minusSeconds(1));
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      assertFalse(Thread.holdsLock(room), "Socket close must not own the room monitor");
      closing.countDown();
      assertTrue(release.await(5, TimeUnit.SECONDS));
      return null;
    }).when(sender).close(any());
    var executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(service::removeExpiredRooms).get(2, TimeUnit.SECONDS);
      assertTrue(closing.await(2, TimeUnit.SECONDS));
      assertFalse(rooms().containsKey(code));
      String next = createRoom();
      rooms().get(next).setExpiresAt(LocalDateTime.now().minusSeconds(1));
      executor.submit(service::removeExpiredRooms).get(2, TimeUnit.SECONDS);
      assertFalse(rooms().containsKey(next));
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void expiryRemovesRoomEvenWhileSendIsBlocked() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    join("sender", sender, code);
    join("receiver", receiver, code);
    clearInvocations(sender, receiver);
    CountDownLatch sending = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      sending.countDown();
      assertTrue(release.await(5, TimeUnit.SECONDS));
      return null;
    }).when(receiver).sendMessage(any());
    var executor = Executors.newFixedThreadPool(2);
    try {
      var blocked = executor.submit(() -> service.forwardMessage(code, "sender", sender,
          WebRTCMessage.builder().type("offer").build()));
      assertTrue(sending.await(2, TimeUnit.SECONDS));
      rooms().get(code).setExpiresAt(LocalDateTime.now().minusSeconds(1));
      executor.submit(service::removeExpiredRooms).get(2, TimeUnit.SECONDS);
      assertFalse(rooms().containsKey(code));
      release.countDown();
      blocked.get(2, TimeUnit.SECONDS);
      verify(sender, timeout(2000)).close(com.file_drop.constant.SignalingError.ROOM_EXPIRED.closeStatus());
      verify(receiver, timeout(2000)).close(com.file_drop.constant.SignalingError.ROOM_EXPIRED.closeStatus());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void saturatedCleanupExecutorDefersCloseWithoutBlockingScheduler() throws Exception {
    ThreadPoolExecutor cleanup = (ThreadPoolExecutor) ReflectionTestUtils.getField(service, "cleanupExecutor");
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    Runnable block = () -> {
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      cleanup.execute(block);
      cleanup.execute(block);
      assertTrue(started.await(2, TimeUnit.SECONDS));
      for (int i = 0; i < 128; i++) cleanup.execute(() -> {});
      WebSocketSession sender = session("sender");
      join("sender", sender, code);
      rooms().get(code).setExpiresAt(LocalDateTime.now().minusSeconds(1));
      service.removeExpiredRooms();
      assertFalse(rooms().containsKey(code));
      verify(sender, never()).close(any());
      release.countDown();
      org.awaitility.Awaitility.await().atMost(2, TimeUnit.SECONDS)
          .until(() -> cleanup.getQueue().isEmpty());
      service.removeExpiredRooms();
      verify(sender, timeout(2000)).close(com.file_drop.constant.SignalingError.ROOM_EXPIRED.closeStatus());
    } finally {
      release.countDown();
    }
  }

  @Test
  void slowPeerQueueOverflowRetiresRoomAndDiscardsBacklog() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    join("sender", sender, code);
    join("receiver", receiver, code);
    clearInvocations(sender, receiver);
    CountDownLatch sending = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      sending.countDown();
      assertTrue(release.await(5, TimeUnit.SECONDS));
      return null;
    }).when(receiver).sendMessage(any());
    var executor = Executors.newSingleThreadExecutor();
    try {
      var blocked = executor.submit(() -> service.forwardMessage(code, "sender", sender,
          WebRTCMessage.builder().type("offer").build()));
      assertTrue(sending.await(2, TimeUnit.SECONDS));
      for (int i = 0; i < 257; i++) {
        service.forwardMessage(code, "sender", sender, WebRTCMessage.builder().type("candidate").build());
      }
      assertFalse(rooms().containsKey(code));
      release.countDown();
      blocked.get(2, TimeUnit.SECONDS);
      verify(receiver, times(1)).sendMessage(any());
      verify(sender).close(CloseStatus.SESSION_NOT_RELIABLE);
      verify(receiver).close(CloseStatus.SESSION_NOT_RELIABLE);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void rejectedConnectionCloseDoesNotHoldRoomLock() throws Exception {
    join("sender", session("sender"), code);
    WebRTCRoom room = rooms().get(code);
    WebSocketSession rejected = session("rejected");
    doAnswer(invocation -> {
      assertFalse(Thread.holdsLock(room));
      return null;
    }).when(rejected).close(any());
    assertFalse(join("sender", rejected, code));
    service.forwardMessage(code, "sender", rejected, WebRTCMessage.builder().type("offer").build());
    verify(rejected).close(com.file_drop.constant.SignalingError.ROLE_OCCUPIED.closeStatus());
    verify(rejected).close(CloseStatus.POLICY_VIOLATION);
  }
}
