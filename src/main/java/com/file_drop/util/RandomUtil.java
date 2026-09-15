package com.file_drop.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.security.SecureRandom;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RandomUtil {
  private static final String chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * 生成不随机房间码
   * @return 房间码
   */
  public static String generateCode(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      int index = RANDOM.nextInt(chars.length());
      sb.append(chars.charAt(index));
    }
    return sb.toString();
  }

  /**
   * 生成不随机房间码
   * @return 房间码
   */
  public static String generateCodeInt(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      int index = RANDOM.nextInt(10);
      sb.append(chars.charAt(index));
    }
    return sb.toString();
  }

  public static String generateClientID() {
    return String.format("webrtc_client_%s", generateCode(10));
  }

}
