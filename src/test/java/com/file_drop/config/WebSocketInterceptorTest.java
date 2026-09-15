package com.file_drop.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;

import java.net.URI;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebSocketInterceptorTest {
  @ParameterizedTest
  @ValueSource(strings = {"", "?code", "?code=&role=sender", "?code=abc123&role",
      "?code=abc123&role=admin", "?code=abc123&role=sender&role=receiver",
      "?code=abc123&code=def456&role=sender", "?code=abc&role=sender"})
  void rejectsInvalidParameters(String query) {
    var request = mock(ServerHttpRequest.class);
    var response = mock(ServerHttpResponse.class);
    when(request.getURI()).thenReturn(URI.create("http://localhost/ws" + query));
    assertFalse(new WebSocketInterceptor().beforeHandshake(request, response,
        mock(WebSocketHandler.class), new HashMap<>()));
    verify(response).setStatusCode(HttpStatus.BAD_REQUEST);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sender", "%73ender"})
  void acceptsAndDecodesValidParameters(String role) {
    var request = mock(ServerHttpRequest.class);
    when(request.getURI()).thenReturn(URI.create("http://localhost/ws?code=abc123&role=" + role + "&extra=value"));
    var attributes = new HashMap<String, Object>();
    assertTrue(new WebSocketInterceptor().beforeHandshake(request, mock(ServerHttpResponse.class),
        mock(WebSocketHandler.class), attributes));
    assertEquals("sender", attributes.get("role"));
    assertEquals("abc123", attributes.get("code"));
    assertEquals(2, attributes.size());
  }
}
