package com.file_drop.entity;

import lombok.Data;
import org.springframework.web.socket.WebSocketSession;

import java.io.Serializable;

@Data
public class WebRTCClient implements Serializable {

  private String id;
  private String role;
  private WebSocketSession conn;
  private String room;

}
