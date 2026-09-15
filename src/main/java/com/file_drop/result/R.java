package com.file_drop.result;

import lombok.Data;

@Data
public class R<T> {

  private int code;
  private String message;
  private T data;

  public static <T> R<T> success(String message, T data) {
    return r(200, message, data);
  }

  public static <T> R<T> success(T data) {
    return r(200, "success", data);
  }

  public static <T> R<T> success() {
    return r(200, "success", null);
  }

  public static <T> R<T> fail(Integer code, String message) {
    return r(code, message, null);
  }

  public static <T> R<T> fail(String message) {
    return r(500, message, null);
  }

  public static <T> R<T> fail() {
    return r(500, "Unknown exception", null);
  }

  private static <T> R<T> r(Integer code, String message, T data) {
    R<T> r = new R<>();
    r.setCode(code);
    r.setMessage(message);
    r.setData(data);
    return r;
  }

}
