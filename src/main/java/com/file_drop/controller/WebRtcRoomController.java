package com.file_drop.controller;

import com.file_drop.result.R;
import com.file_drop.entity.CreatedRoom;
import jakarta.servlet.http.HttpServletResponse;
import com.file_drop.service.WebRtcService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/web")
@RequiredArgsConstructor
public class WebRtcRoomController {

  private final WebRtcService webRtcService;

  @PostMapping("/createRoom")
  public R<CreatedRoom> createRoom(@RequestParam String type, HttpServletResponse response) {
    response.setHeader("Cache-Control", "no-store");
    return R.success(webRtcService.createRoom(type));
  }


}
