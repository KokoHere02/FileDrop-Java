package com.file_drop.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public final class SenderCredential {
  private static final SecureRandom RANDOM = new SecureRandom();
  private SenderCredential() {}

  public static String generate() {
    byte[] bytes = new byte[32];
    RANDOM.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  public static byte[] digest(String token) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.US_ASCII));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  public static boolean matches(String token, byte[] expected) {
    return token != null && token.matches("[A-Za-z0-9_-]{43}")
        && MessageDigest.isEqual(expected, digest(token));
  }
}
