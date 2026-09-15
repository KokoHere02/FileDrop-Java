package com.file_drop.service;

import com.file_drop.entity.WebRTCClient;
import com.file_drop.entity.WebRTCMessage;
import com.file_drop.util.JsonUtil;
import com.file_drop.util.RandomUtil;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.io.IOException;
import java.time.LocalDateTime;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.file_drop.constant.CommonConstant.*;

@Slf4j
@Service
public class WebRtcService {
  private static final int MAX_PENDING_MESSAGES = 256;
  private static final CloseStatus HEARTBEAT_TIMEOUT = new CloseStatus(4001, "Heartbeat timeout");
  @Value("${app.heartbeat.interval-ms:20000}")
  private long heartbeatIntervalMs = 20000;
  @Value("${app.heartbeat.timeout-ms:60000}")
  private long heartbeatTimeoutMs = 60000;
  private final LongSupplier nanoTime;
  private final AtomicLong probeSequence = new AtomicLong();

  public WebRtcService() { this(System::nanoTime); }

  WebRtcService(LongSupplier nanoTime) { this.nanoTime = nanoTime; }

  @PostConstruct
  void validateHeartbeatSettings() {
    if (heartbeatIntervalMs <= 0 || heartbeatTimeoutMs <= 0) {
      throw new IllegalArgumentException("Heartbeat interval and timeout must be positive");
    }
  }
  private final ConcurrentHashMap<String, RoomState> rooms = new ConcurrentHashMap<>();
  // Retain deferred cleanup when the bounded executor is full; the next sweep retries it.
  private final Set<RoomState> pendingCleanup = ConcurrentHashMap.newKeySet();
  private final ThreadPoolExecutor cleanupExecutor = new ThreadPoolExecutor(
      2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(128), task -> {
        Thread thread = new Thread(task, "webrtc-room-cleanup");
        thread.setDaemon(true);
        return thread;
      });

  public String createRoom(String type) {
    if (type == null || type.isBlank() || type.length() > 64) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "type must contain 1 to 64 characters");
    }
    while (true) {
      String code = RandomUtil.generateCode(6);
      RoomState room = new RoomState(code);
      room.setType(type.trim());
      if (rooms.putIfAbsent(code, room) == null) return code;
    }
  }

  public boolean addClient(String role, WebSocketSession session, String code) {
    RoomState room = code == null ? null : rooms.get(code);
    if (!validRole(role) || room == null) {
      close(session, CloseStatus.POLICY_VIOLATION);
      return false;
    }
    boolean accepted;
    synchronized (room) {
      accepted = active(room) && client(room, role) == null;
      if (accepted) {
        WebRTCClient joined = new WebRTCClient();
        joined.setId(RandomUtil.generateClientID());
        joined.setRole(role);
        joined.setRoom(code);
        joined.setConn(new ConcurrentWebSocketSessionDecorator(session, 10000, 64 * 1024));
        room.heartbeats.put(session.getId(), new RoomState.Heartbeat(nanoTime.getAsLong()));
        if (SENDER.equals(role)) room.setSender(joined);
        else room.setReceiver(joined);
        WebRTCClient peer = peer(room, role);
        if (peer != null) enqueue(room, peer, WebRTCMessage.builder()
            .type(JOINED).from(joined.getId()).to(peer.getId()).payload(JOINED).build());
      }
    }
    if (!accepted) close(session, CloseStatus.POLICY_VIOLATION);
    else drain(room);
    return accepted;
  }

  public void removeClient(String code, String role, WebSocketSession session) {
    RoomState room = code == null ? null : rooms.get(code);
    if (room == null || !validRole(role)) return;
    synchronized (room) {
      WebRTCClient leaving = client(room, role);
      if (!owns(leaving, session)) return;
      room.heartbeats.remove(session.getId());
      if (SENDER.equals(role)) room.setSender(null);
      else room.setReceiver(null);
      WebRTCClient peer = peer(room, role);
      if (peer != null) enqueue(room, peer, WebRTCMessage.builder()
          .type(RESET).from(leaving.getId()).to(peer.getId()).payload(RESET).build());
    }
    drain(room);
  }

  public void forwardMessage(String code, String role, WebSocketSession session, WebRTCMessage message) {
    RoomState room = code == null ? null : rooms.get(code);
    if (room == null || !validRole(role)) return;
    boolean accepted;
    synchronized (room) {
      WebRTCClient source = client(room, role);
      accepted = active(room) && owns(source, session);
      if (accepted) {
        WebRTCClient target = peer(room, role);
        if (target != null) {
          message.setFrom(source.getId());
          message.setTo(target.getId());
          enqueue(room, target, message);
        }
      }
    }
    if (!accepted) close(session, CloseStatus.POLICY_VIOLATION);
    else drain(room);
  }

  @Scheduled(fixedDelay = 60000)
  public void removeExpiredRooms() {
    rooms.forEach((code, room) -> {
      synchronized (room) {
        if (!room.getExpiresAt().isAfter(LocalDateTime.now())) {
          retire(room, CloseStatus.NORMAL);
        }
      }
    });
    dispatchPending();
  }

  public void receivePong(String code, String role, WebSocketSession session, ByteBuffer payload) {
    RoomState room = code == null ? null : rooms.get(code);
    if (room == null || !validRole(role)) return;
    synchronized (room) {
      if (!owns(client(room, role), session)) return;
      RoomState.Heartbeat heartbeat = room.heartbeats.get(session.getId());
      if (heartbeat == null || heartbeat.pending == null || payload.remaining() != heartbeat.pending.length) return;
      byte[] echoed = new byte[payload.remaining()];
      payload.duplicate().get(echoed);
      if (Arrays.equals(echoed, heartbeat.pending)) {
        heartbeat.pending = null;
        heartbeat.since = nanoTime.getAsLong();
      }
    }
  }

  @Scheduled(fixedDelayString = "${app.heartbeat.scan-ms:5000}")
  public void checkHeartbeats() {
    long now = nanoTime.getAsLong();
    rooms.forEach((code, room) -> {
      synchronized (room) {
        if (!active(room)) return;
        checkHeartbeat(room, room.getSender(), now);
        checkHeartbeat(room, room.getReceiver(), now);
        if (!room.deliveries.isEmpty()) pendingCleanup.add(room);
      }
    });
    dispatchPending();
  }

  private void checkHeartbeat(RoomState room, WebRTCClient client, long now) {
    if (client == null) return;
    RoomState.Heartbeat heartbeat = room.heartbeats.get(client.getConn().getId());
    if (heartbeat == null) return;
    if (heartbeat.pending != null) {
      if (now - heartbeat.since < TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMs)) return;
      // Release the role before any I/O, even when a previous send is still blocked.
      room.heartbeats.remove(client.getConn().getId());
      if (SENDER.equals(client.getRole())) room.setSender(null);
      else room.setReceiver(null);
      room.deliveries.removeIf(delivery -> delivery.session().getId().equals(client.getConn().getId())
          && delivery.message() != null);
      WebRTCClient peer = peer(room, client.getRole());
      if (peer != null) enqueue(room, peer, WebRTCMessage.builder().type(RESET)
          .from(client.getId()).to(peer.getId()).payload(RESET).build());
      room.deliveries.add(new RoomState.Delivery(client.getConn(), client.getRole(), null, HEARTBEAT_TIMEOUT));
    } else if (now - heartbeat.since >= TimeUnit.MILLISECONDS.toNanos(heartbeatIntervalMs)) {
      if (room.deliveries.size() >= MAX_PENDING_MESSAGES) {
        retire(room, CloseStatus.SESSION_NOT_RELIABLE);
        return;
      }
      heartbeat.pending = ByteBuffer.allocate(Long.BYTES).putLong(probeSequence.incrementAndGet()).array();
      // Deadline includes queueing, so a stuck writer cannot retain a role forever.
      heartbeat.since = now;
      room.deliveries.add(new RoomState.Delivery(client.getConn(), client.getRole(),
          new PingMessage(ByteBuffer.wrap(heartbeat.pending)), null));
    }
  }

  private void dispatchPending() {
    // Never perform socket I/O on the scheduler, even if the worker queue is full.
    pendingCleanup.forEach(room -> {
      if (!pendingCleanup.remove(room)) return;
      try {
        cleanupExecutor.execute(() -> drain(room));
      } catch (RejectedExecutionException e) {
        pendingCleanup.add(room);
      }
    });
  }

  /** Called only under the room lock; serializes an immutable message snapshot. */
  private void enqueue(RoomState room, WebRTCClient target, WebRTCMessage message) {
    if (room.deliveries.size() >= MAX_PENDING_MESSAGES) {
      // A slow peer must not allow the new application-level FIFO to grow indefinitely.
      retire(room, CloseStatus.SESSION_NOT_RELIABLE);
      return;
    }
    room.deliveries.add(new RoomState.Delivery(target.getConn(), target.getRole(),
        new TextMessage(JsonUtil.toJson(message)), null));
  }

  /** Detach state immediately, then let the current drainer or cleanup worker close sockets. */
  private void retire(RoomState room, CloseStatus status) {
    if (!rooms.remove(room.getCode(), room)) return;
    WebRTCClient sender = room.getSender();
    WebRTCClient receiver = room.getReceiver();
    room.setSender(null);
    room.setReceiver(null);
    room.heartbeats.clear();
    room.deliveries.clear();
    if (sender != null) room.deliveries.add(new RoomState.Delivery(sender.getConn(), SENDER, null, status));
    if (receiver != null) room.deliveries.add(new RoomState.Delivery(receiver.getConn(), RECEIVER, null, status));
    pendingCleanup.add(room);
  }

  /** One caller drains a room at a time. Other callers only append and return. */
  private void drain(RoomState room) {
    synchronized (room) {
      if (room.draining) return;
      room.draining = true;
    }
    while (true) {
      RoomState.Delivery delivery;
      synchronized (room) {
        delivery = room.deliveries.poll();
        if (delivery == null) {
          room.draining = false;
          pendingCleanup.remove(room);
          return;
        }
        // Queued data belongs to a concrete session, never a replacement occupant.
        if (delivery.message() != null && !owns(client(room, delivery.role()), delivery.session())) {
          continue;
        }
      }
      if (delivery.closeStatus() != null) {
        close(delivery.session(), delivery.closeStatus());
      } else {
        send(room, delivery);
      }
    }
  }

  private boolean active(RoomState room) {
    return rooms.get(room.getCode()) == room && room.getExpiresAt().isAfter(LocalDateTime.now());
  }

  private boolean validRole(String role) {
    return SENDER.equals(role) || RECEIVER.equals(role);
  }

  private WebRTCClient client(RoomState room, String role) {
    return SENDER.equals(role) ? room.getSender() : room.getReceiver();
  }

  private WebRTCClient peer(RoomState room, String role) {
    return SENDER.equals(role) ? room.getReceiver() : room.getSender();
  }

  private boolean owns(WebRTCClient client, WebSocketSession session) {
    return client != null && client.getConn().getId().equals(session.getId());
  }

  private void send(RoomState room, RoomState.Delivery delivery) {
    try {
      if (delivery.session().isOpen()) {
        delivery.session().sendMessage(delivery.message());
      } else {
        removeClient(room.getCode(), delivery.role(), delivery.session());
      }
    } catch (IOException | RuntimeException e) {
      log.warn("Failed to send signaling message to session {}", delivery.session().getId(), e);
      removeClient(room.getCode(), delivery.role(), delivery.session());
      close(delivery.session(), CloseStatus.SERVER_ERROR);
    }
  }

  private void close(WebSocketSession session, CloseStatus status) {
    try {
      session.close(status);
    } catch (IOException | RuntimeException e) {
      log.warn("Failed to close session {}", session.getId(), e);
    }
  }

  @PreDestroy
  public void shutdown() {
    cleanupExecutor.shutdownNow();
  }
}
