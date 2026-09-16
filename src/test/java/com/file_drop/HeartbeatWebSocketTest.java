package com.file_drop;

import com.file_drop.service.WebRtcService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/** Raw client lets us deliberately stop replying to protocol-level Ping frames. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "app.heartbeat.interval-ms=100", "app.heartbeat.timeout-ms=1500", "app.heartbeat.scan-ms=25"
})
@DirtiesContext
class HeartbeatWebSocketTest {
  @LocalServerPort private int port;
  @Autowired private WebRtcService service;

  private final java.util.Map<String, String> senderTokens = new java.util.concurrent.ConcurrentHashMap<>();

  private String createRoom() {
    var created = service.createRoom("file");
    senderTokens.put(created.code(), created.senderToken());
    return created.code();
  }


  @Test
  void realPongsKeepSessionAliveAndMissingPongAllowsReplacement() throws Exception {
    String code = createRoom();
    try (Socket socket = connect(code)) {
      assertEquals("accepted", readSignal(socket).getType());
      for (int i = 0; i < 3; i++) {
        Frame ping = readFrame(socket);
        assertEquals(9, ping.opcode());
        sendPong(socket, ping.payload());
      }
      assertEquals(9, readFrame(socket).opcode());
      // Intentionally leave the fourth probe unanswered.
      Frame close = readFrame(socket);
      assertEquals(8, close.opcode());
      assertEquals(4001, ByteBuffer.wrap(close.payload()).getShort());
      try (Socket replacement = connect(code)) {
        assertEquals("accepted", readSignal(replacement).getType());
        assertEquals(9, readFrame(replacement).opcode());
      }
    }
  }

  private Socket connect(String code) throws Exception {
    return connect(code, "sender");
  }

  private Socket connect(String code, String role) throws Exception {
    return connect(code, role, senderTokens.get(code));
  }

  private Socket connect(String code, String role, String senderToken) throws Exception {
    Socket socket = new Socket("127.0.0.1", port);
    socket.setSoTimeout(5000);
    try {
      String request = "GET /api/ws?code=" + code + "&role=" + role
          + (senderToken == null ? "" : "&senderToken=" + senderToken) + " HTTP/1.1\r\n"
          + "Host: localhost:" + port + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
          + "Origin: http://localhost:5173\r\nSec-WebSocket-Version: 13\r\n"
          + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
      socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
      StringBuilder headers = new StringBuilder();
      while (!headers.toString().endsWith("\r\n\r\n")) {
        int value = socket.getInputStream().read();
        assertNotEquals(-1, value);
        headers.append((char) value);
        assertTrue(headers.length() < 8192);
      }
      assertTrue(headers.toString().startsWith("HTTP/1.1 101"), headers.toString());
      return socket;
    } catch (Throwable error) {
      socket.close();
      throw error;
    }
  }

  private Frame readFrame(Socket socket) throws Exception {
    var input = socket.getInputStream();
    int first = input.read();
    int length = input.read();
    assertTrue(first >= 0 && length >= 0 && length < 128, "Expected an unmasked frame");
    if (length == 126) length = (input.read() << 8) | input.read();
    else assertNotEquals(127, length, "Unexpected large frame");
    byte[] payload = input.readNBytes(length);
    assertEquals(length, payload.length);
    return new Frame(first & 15, payload);
  }

  private void sendPong(Socket socket, byte[] payload) throws Exception {
    byte[] mask = new byte[4];
    ThreadLocalRandom.current().nextBytes(mask);
    var output = socket.getOutputStream();
    output.write(0x8a);
    output.write(0x80 | payload.length);
    output.write(mask);
    for (int i = 0; i < payload.length; i++) output.write(payload[i] ^ mask[i % 4]);
    output.flush();
  }

  private record Frame(int opcode, byte[] payload) {}





  private com.file_drop.entity.WebRTCMessage readSignal(Socket socket) throws Exception {
    Frame frame = readFrame(socket);
    while (frame.opcode() == 9) {
      sendPong(socket, frame.payload());
      frame = readFrame(socket);
    }
    assertEquals(1, frame.opcode());
    return com.file_drop.util.JsonUtil.fromJson(new String(frame.payload(), StandardCharsets.UTF_8),
        com.file_drop.entity.WebRTCMessage.class);
  }

  @Test
  void bothJoinOrdersReceiveAcceptedBeforeReadyWithOneInitiator() throws Exception {
    for (String role : new String[]{"sender", "receiver"}) {
      String code = createRoom();
      try (Socket first = connect(code, role)) {
        var acceptedFirst = readSignal(first);
        assertEquals("accepted", acceptedFirst.getType());
        try (Socket second = connect(code, role.equals("sender") ? "receiver" : "sender")) {
          var acceptedSecond = readSignal(second);
          assertEquals("accepted", acceptedSecond.getType());
          var readyFirst = readSignal(first);
          var readySecond = readSignal(second);
          assertEquals("peer-ready", readyFirst.getType());
          assertEquals("peer-ready", readySecond.getType());
          assertEquals(true, ((java.util.Map<?, ?>) readyFirst.getPayload()).get("initiator"));
          assertEquals(false, ((java.util.Map<?, ?>) readySecond.getPayload()).get("initiator"));
          assertEquals(acceptedFirst.getTo(), readySecond.getFrom());
          assertEquals(acceptedSecond.getTo(), readyFirst.getFrom());
        }
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void rejectionSendsSpecificErrorBeforeCorrespondingClose() throws Exception {
    String occupied = createRoom();
    try (Socket first = connect(occupied)) {
      assertEquals("accepted", readSignal(first).getType());
      assertRejection(occupied, "ROLE_OCCUPIED", 4409);
    }
    assertRejection("000000", "ROOM_NOT_FOUND", 4404);
    String expired = createRoom();
    var rooms = (java.util.Map<String, com.file_drop.entity.WebRTCRoom>)
        org.springframework.test.util.ReflectionTestUtils.getField(service, "rooms");
    rooms.get(expired).setExpiresAt(java.time.LocalDateTime.now().minusSeconds(1));
    assertRejection(expired, "ROOM_EXPIRED", 4410);
    service.removeExpiredRooms();
    assertRejection(expired, "ROOM_EXPIRED", 4410);
  }

  private void assertRejection(String code, String errorCode, int closeCode) throws Exception {
    try (Socket socket = connect(code)) {
      var error = readSignal(socket);
      assertEquals("error", error.getType());
      assertEquals(errorCode, ((java.util.Map<?, ?>) error.getPayload()).get("code"));
      Frame close = readFrame(socket);
      assertEquals(8, close.opcode());
      assertEquals(closeCode, ByteBuffer.wrap(close.payload()).getShort());
    }
  }

  @Test
  void realSenderRequiresCredentialWhileReceiverCanUseOnlyRoomCode() throws Exception {
    String code = createRoom();
    for (String token : new String[]{null, "wrong", service.createRoom("file").senderToken()}) {
      try (Socket rejected = connect(code, "sender", token)) {
        var error = readSignal(rejected);
        assertEquals("SENDER_UNAUTHORIZED", ((java.util.Map<?, ?>) error.getPayload()).get("code"));
        Frame close = readFrame(rejected);
        assertEquals(8, close.opcode());
        assertEquals(4403, ByteBuffer.wrap(close.payload()).getShort());
      }
    }
    try (Socket receiver = connect(code, "receiver", null)) {
      assertEquals("accepted", readSignal(receiver).getType());
      try (Socket sender = connect(code)) {
        assertEquals("accepted", readSignal(sender).getType());
        assertEquals("peer-ready", readSignal(sender).getType());
        assertEquals("peer-ready", readSignal(receiver).getType());
      }
    }
  }
}
