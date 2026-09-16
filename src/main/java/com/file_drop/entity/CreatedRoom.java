package com.file_drop.entity;

/** Returned only to the creator; never include the credential in signaling messages. */
public record CreatedRoom(String code, String senderToken) {
  @Override
  public String toString() { return "CreatedRoom[code=" + code + ", senderToken=<redacted>]"; }
}
