package com.file_drop.controller;

import com.file_drop.result.R;
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
  public R<String> createRoom(@RequestParam String type) {
    return R.success(webRtcService.createRoom(type));
  }


}