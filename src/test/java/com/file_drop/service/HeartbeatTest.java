package com.file_drop.service;

import com.file_drop.entity.WebRTCMessage;
import com.file_drop.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.socket.*;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HeartbeatTest {
  private final AtomicLong now = new AtomicLong();
  private final WebRtcService service = new WebRtcService(now::get);

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
  void stop() throws Exception {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) ReflectionTestUtils.getField(service, "cleanupExecutor");
    executor.shutdown();
    try { assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS)); }
    finally { service.shutdown(); }
  }

  private WebSocketSession session(String id) {
    WebSocketSession session = mock(WebSocketSession.class);
    when(session.getId()).thenReturn(id);
    when(session.isOpen()).thenReturn(true);
    return session;
  }

  private void advance(long seconds) {
    now.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    service.checkHeartbeats();
  }

  private PingMessage ping(WebSocketSession session) throws Exception {
    var capture = org.mockito.ArgumentCaptor.forClass(PingMessage.class);
    verify(session, timeout(2000)).sendMessage(capture.capture());
    return capture.getValue();
  }

  @Test
  void matchingPongKeepsConnectionAndNextProbeHasNewToken() throws Exception {
    WebSocketSession sender = session("sender");
    join("sender", sender, code);
    advance(19);
    verify(sender, never()).sendMessage(any(PingMessage.class));
    advance(1);
    PingMessage first = ping(sender);
    service.receivePong(code, "sender", sender, first.getPayload());
    advance(20);
    var capture = org.mockito.ArgumentCaptor.forClass(PingMessage.class);
    verify(sender, timeout(2000).times(2)).sendMessage(capture.capture());
    assertNotEquals(first.getPayload(), capture.getValue().getPayload());
    service.receivePong(code, "sender", sender, capture.getValue().getPayload());
    verify(sender, never()).close(any());
    assertFalse(join("sender", session("duplicate"), code));
  }

  @Test
  void timeoutReleasesRoleNotifiesPeerAndIgnoresOldCallbacks() throws Exception {
    WebSocketSession sender = session("sender");
    WebSocketSession receiver = session("receiver");
    join("sender", sender, code);
    join("receiver", receiver, code);
    advance(20);
    PingMessage oldPing = ping(sender);
    PingMessage peerPing = ping(receiver);
    now.addAndGet(TimeUnit.SECONDS.toNanos(59));
    service.receivePong(code, "receiver", receiver, peerPing.getPayload());
    advance(1);
    verify(sender, timeout(2000)).close(argThat(status -> status.getCode() == 4001));
    verify(receiver, timeout(2000)).sendMessage(argThat(message -> message instanceof TextMessage
        && JsonUtil.fromJson(((TextMessage) message).getPayload(), WebRTCMessage.class).getType().equals("reset")));
    WebSocketSession replacement = session("replacement");
    assertTrue(join("sender", replacement, code));
    service.removeClient(code, "sender", sender);
    service.receivePong(code, "sender", sender, oldPing.getPayload());
    assertFalse(join("sender", session("duplicate"), code));
    verify(receiver, never()).close(any());
  }

  @Test
  void unsolicitedMismatchedAndStalePongsDoNotRefreshDeadline() throws Exception {
    WebSocketSession sender = session("sender");
    join("sender", sender, code);
    service.receivePong(code, "sender", sender, ByteBuffer.wrap(new byte[]{1}));
    advance(20);
    PingMessage first = ping(sender);
    service.receivePong(code, "sender", sender, first.getPayload());
    advance(20);
    verify(sender, timeout(2000).times(2)).sendMessage(any(PingMessage.class));
    service.receivePong(code, "sender", sender, first.getPayload());
    service.receivePong(code, "sender", sender, ByteBuffer.wrap(new byte[]{1}));
    advance(60);
    verify(sender, timeout(2000)).close(argThat(status -> status.getCode() == 4001));
    assertTrue(join("sender", session("replacement"), code));
  }

  @Test
  void blockedPingDoesNotBlockScanOrRoleRelease() throws Exception {
    WebSocketSession sender = session("sender");
    join("sender", sender, code);
    Map<?, ?> rooms = (Map<?, ?>) ReflectionTestUtils.getField(service, "rooms");
    Object room = rooms.get(code);
    CountDownLatch sending = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      assertFalse(Thread.holdsLock(room));
      sending.countDown();
      assertTrue(release.await(5, TimeUnit.SECONDS));
      return null;
    }).when(sender).sendMessage(any(PingMessage.class));
    var scanner = Executors.newSingleThreadExecutor();
    try {
      scanner.submit(() -> advance(20)).get(2, TimeUnit.SECONDS);
      assertTrue(sending.await(2, TimeUnit.SECONDS));
      scanner.submit(() -> advance(60)).get(2, TimeUnit.SECONDS);
      assertTrue(join("sender", session("replacement"), code));
      release.countDown();
      verify(sender, timeout(2000)).close(argThat(status -> status.getCode() == 4001));
    } finally {
      release.countDown();
      scanner.shutdownNow();
    }
  }

  @Test
  void pingSendFailureReleasesRole() throws Exception {
    WebSocketSession sender = session("sender");
    join("sender", sender, code);
    doThrow(new java.io.IOException("network lost")).when(sender).sendMessage(any(PingMessage.class));
    advance(20);
    verify(sender, timeout(2000)).close(CloseStatus.SERVER_ERROR);
    assertTrue(join("sender", session("replacement"), code));
  }
}
