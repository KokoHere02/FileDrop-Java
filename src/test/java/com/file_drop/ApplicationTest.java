package com.file_drop;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class ApplicationTest {
  @Autowired
  private MockMvc mvc;

  @Test
  void createsRoomWithExistingResponseShape() throws Exception {
    mvc.perform(post("/web/createRoom").param("type", "file"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.code").value(200))
        .andExpect(jsonPath("$.data").value(org.hamcrest.Matchers.matchesPattern("[a-zA-Z0-9]{6}")));
  }

  @Test
  void rejectsMissingAndBlankType() throws Exception {
    mvc.perform(post("/web/createRoom")).andExpect(status().isBadRequest());
    mvc.perform(post("/web/createRoom").param("type", " ")).andExpect(status().isBadRequest());
  }

  @Test
  void allowsConfiguredOriginAndRejectsUnknownOrigin() throws Exception {
    mvc.perform(options("/web/createRoom").header("Origin", "http://localhost:5173")
        .header("Access-Control-Request-Method", "POST"))
        .andExpect(status().isOk())
        .andExpect(header().string("Access-Control-Allow-Origin", "http://localhost:5173"));
    mvc.perform(options("/web/createRoom").header("Origin", "https://untrusted.example")
        .header("Access-Control-Request-Method", "POST"))
        .andExpect(status().isForbidden());
  }
}
