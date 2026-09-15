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

  @Test
  void realPongsKeepSessionAliveAndMissingPongAllowsReplacement() throws Exception {
    String code = service.createRoom("file");
    try (Socket socket = connect(code)) {
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
        assertEquals(9, readFrame(replacement).opcode());
      }
    }
  }

  private Socket connect(String code) throws Exception {
    Socket socket = new Socket("127.0.0.1", port);
    socket.setSoTimeout(5000);
    try {
      String request = "GET /api/ws?code=" + code + "&role=sender HTTP/1.1\r\n"
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
    assertTrue(first >= 0 && length >= 0 && length <= 125, "Expected a small unmasked control frame");
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
}
