package com.file_drop.service;

import com.file_drop.entity.WebRTCMessage;
import com.file_drop.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MultiReceiverTest {
  private final AtomicLong time = new AtomicLong();
  private final WebRtcService service = new WebRtcService(time::get);
  private final com.file_drop.entity.CreatedRoom room = service.createRoom("file");
  private final Map<WebSocketSession, List<WebRTCMessage>> messages = new ConcurrentHashMap<>();

  @AfterEach
  void stop() throws Exception {
    var executor = (ThreadPoolExecutor) org.springframework.test.util.ReflectionTestUtils.getField(service, "cleanupExecutor");
    executor.shutdown();
    try { assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS)); }
    finally { service.shutdown(); }
  }

  private WebSocketSession join(String role) throws Exception {
    WebSocketSession session = mock(WebSocketSession.class);
    when(session.getId()).thenReturn(UUID.randomUUID().toString());
    when(session.isOpen()).thenReturn(true);
    messages.put(session, new CopyOnWriteArrayList<>());
    doAnswer(call -> {
      Object message = call.getArgument(0);
      if (message instanceof TextMessage text) messages.get(session).add(JsonUtil.fromJson(text.getPayload(), WebRTCMessage.class));
      return null;
    }).when(session).sendMessage(any());
    assertTrue(service.addClient(role, session, room.code(), room.senderToken()));
    return session;
  }

  private String id(WebSocketSession session) { return messages.get(session).get(0).getTo(); }
  private void send(WebSocketSession session, String role, String to) {
    service.forwardMessage(room.code(), role, session, WebRTCMessage.builder()
        .type("candidate").from("forged").to(to).payload("candidate-data").build());
  }
  private List<WebRTCMessage> type(WebSocketSession session, String type) {
    return messages.get(session).stream().filter(m -> m.getType().equals(type)).toList();
  }

  @Test
  void receiversCanJoinFirstAndEachPairsOnlyWithSender() throws Exception {
    var a = join("receiver"); var b = join("receiver");
    assertEquals(1, messages.get(a).size());
    assertEquals(1, messages.get(b).size());
    var sender = join("sender");
    assertEquals(2, type(sender, "peer-ready").size());
    for (var receiver : List.of(a, b)) {
      var ready = type(receiver, "peer-ready").get(0);
      assertEquals(id(sender), ready.getFrom());
      assertEquals(false, ((Map<?, ?>) ready.getPayload()).get("initiator"));
    }
    assertTrue(type(sender, "peer-ready").stream().allMatch(m -> Boolean.TRUE.equals(((Map<?, ?>) m.getPayload()).get("initiator"))));
  }

  @Test
  void routingIsExplicitAndCannotReachOtherReceiversOrRooms() throws Exception {
    var sender = join("sender"); var a = join("receiver"); var b = join("receiver");
    send(sender, "sender", id(a));
    assertEquals(1, type(a, "candidate").size());
    assertEquals(id(sender), type(a, "candidate").get(0).getFrom());
    assertTrue(type(b, "candidate").isEmpty());
    send(a, "receiver", id(sender));
    assertEquals(id(a), type(sender, "candidate").get(0).getFrom());
    send(a, "receiver", id(b));
    assertEquals("TARGET_FORBIDDEN", ((Map<?, ?>) type(a, "error").get(0).getPayload()).get("code"));
    send(sender, "sender", null);
    send(sender, "sender", "foreign-or-stale-id");
    assertEquals(List.of("TARGET_REQUIRED", "TARGET_NOT_FOUND"), type(sender, "error").stream()
        .map(m -> ((Map<?, ?>) m.getPayload()).get("code")).toList());
    assertTrue(type(b, "candidate").isEmpty());
    verify(sender, never()).close(any());
  }

  @Test
  void leavingReceiverDoesNotResetOthersAndOldCallbackCannotRemoveReplacement() throws Exception {
    var sender = join("sender"); var a = join("receiver"); var b = join("receiver");
    service.removeClient(room.code(), "receiver", a);
    assertEquals(id(a), type(sender, "reset").get(0).getFrom());
    assertTrue(type(b, "reset").isEmpty());
    var replacement = join("receiver");
    service.removeClient(room.code(), "receiver", a);
    send(sender, "sender", id(replacement));
    send(sender, "sender", id(b));
    assertEquals(1, type(replacement, "candidate").size());
    assertEquals(1, type(b, "candidate").size());
    assertEquals(1, type(sender, "reset").size());
  }

  @Test
  void senderReconnectPairsWithAllWaitingReceivers() throws Exception {
    var sender = join("sender"); var a = join("receiver"); var b = join("receiver");
    service.removeClient(room.code(), "sender", sender);
    assertEquals(1, type(a, "reset").size()); assertEquals(1, type(b, "reset").size());
    var replacement = join("sender");
    assertEquals(2, type(replacement, "peer-ready").size());
    assertEquals(2, type(a, "peer-ready").size());
    send(a, "receiver", id(sender));
    assertEquals("TARGET_NOT_FOUND", ((Map<?, ?>) type(a, "error").get(0).getPayload()).get("code"));
    send(a, "receiver", id(replacement));
    assertEquals(1, type(replacement, "candidate").size());
  }

  @Test
  void blockedReceiverDoesNotBlockAnotherReceiversSignaling() throws Exception {
    var sender = join("sender"); var slow = join("receiver"); var healthy = join("receiver");
    var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
    doAnswer(call -> { entered.countDown(); assertTrue(release.await(5, TimeUnit.SECONDS)); return null; })
        .when(slow).sendMessage(any(TextMessage.class));
    var pool = Executors.newFixedThreadPool(2);
    try {
      var blocked = pool.submit(() -> send(sender, "sender", id(slow)));
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      pool.submit(() -> send(sender, "sender", id(healthy))).get(2, TimeUnit.SECONDS);
      assertEquals(1, type(healthy, "candidate").size());
      release.countDown(); blocked.get(2, TimeUnit.SECONDS);
    } finally { release.countDown(); pool.shutdownNow(); }
  }

  @Test
  void expiryClosesEveryReceiverAndPreservesPendingDisconnectedClientCleanup() throws Exception {
    var sender = join("sender"); var a = join("receiver"); var b = join("receiver");
    var detached = join("receiver");
    service.removeClient(room.code(), "receiver", detached);
    @SuppressWarnings("unchecked")
    var rooms = (Map<String, RoomState>) org.springframework.test.util.ReflectionTestUtils.getField(service, "rooms");
    var state = rooms.get(room.code());
    synchronized (state) {
      state.deliveries.add(new RoomState.Delivery(detached, "receiver", null, new CloseStatus(4001)));
      state.setExpiresAt(java.time.LocalDateTime.now().minusSeconds(1));
    }
    service.removeExpiredRooms();
    for (var session : List.of(sender, a, b)) {
      verify(session, timeout(2000)).close(argThat(s -> s.getCode() == 4410));
      assertEquals("ROOM_EXPIRED", ((Map<?, ?>) type(session, "error").get(0).getPayload()).get("code"));
    }
    verify(detached, timeout(2000)).close(argThat(s -> s.getCode() == 4001));
    assertFalse(rooms.containsKey(room.code()));
  }

  @Test
  void onlyUnresponsiveReceiverIsRemovedByHeartbeat() throws Exception {
    var sender = join("sender"); var a = join("receiver"); var b = join("receiver");
    time.set(TimeUnit.SECONDS.toNanos(20)); service.checkHeartbeats();
    for (var session : List.of(sender, a, b)) verify(session, timeout(2000)).sendMessage(any(PingMessage.class));
    for (var session : List.of(sender, b)) {
      var capture = org.mockito.ArgumentCaptor.forClass(PingMessage.class);
      verify(session).sendMessage(capture.capture());
      time.set(TimeUnit.SECONDS.toNanos(79));
      service.receivePong(room.code(), session == sender ? "sender" : "receiver", session, capture.getValue().getPayload());
    }
    time.set(TimeUnit.SECONDS.toNanos(80)); service.checkHeartbeats();
    verify(a, timeout(2000)).close(argThat(s -> s.getCode() == 4001));
    verify(b, never()).close(any()); verify(sender, never()).close(any());
    assertTrue(type(b, "reset").isEmpty());
    send(sender, "sender", id(b));
    assertEquals(1, type(b, "candidate").size());
  }
}
