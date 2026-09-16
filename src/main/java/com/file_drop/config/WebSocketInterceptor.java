package com.file_drop.config;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.file_drop.constant.CommonConstant.*;

public class WebSocketInterceptor implements HandshakeInterceptor {
  @Override
  public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
      WebSocketHandler wsHandler, Map<String, Object> attributes) {
    try {
      MultiValueMap<String, String> query = UriComponentsBuilder.fromUri(request.getURI())
          .build().getQueryParams();
      if (query.get(CODE) == null || query.get(CODE).size() != 1
          || query.get(ROLE) == null || query.get(ROLE).size() != 1) {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        return false;
      }
      if (query.getFirst(CODE) == null || query.getFirst(ROLE) == null) {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        return false;
      }
      String code = UriUtils.decode(query.getFirst(CODE), StandardCharsets.UTF_8);
      String role = UriUtils.decode(query.getFirst(ROLE), StandardCharsets.UTF_8);
      if (!code.matches("[0-9a-zA-Z]{6}") || !(SENDER.equals(role) || RECEIVER.equals(role))) {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        return false;
      }
      attributes.put(CODE, code);
      attributes.put(ROLE, role);
      if (query.get(SENDER_TOKEN) != null && query.get(SENDER_TOKEN).size() != 1) {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        return false;
      }
      if (SENDER.equals(role) && query.getFirst(SENDER_TOKEN) != null) {
        attributes.put(SENDER_TOKEN, UriUtils.decode(query.getFirst(SENDER_TOKEN), StandardCharsets.UTF_8));
      }
      return true;
    } catch (IllegalArgumentException e) {
      response.setStatusCode(HttpStatus.BAD_REQUEST);
      return false;
    }
  }

  @Override
  public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
      WebSocketHandler wsHandler, Exception exception) {}
}
