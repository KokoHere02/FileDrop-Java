package com.file_drop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FileDropApplication {
  public static void main(String[] args) {
    SpringApplication.run(FileDropApplication.class, args);
  }
}
