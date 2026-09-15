package com.file_drop.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.Type;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonUtil {

    // 单例 Gson
    private static final Gson GSON = new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .create();

    /**
     * 对象转Json
     * @param obj 对象
     * @return json
     */
    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

    /**
     * json转对象
     * @param json json
     * @param clazz 对象class类型
     * @return 对象
     * @param <T> 返回类型
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }

    /**
     * json转  Map/List
     * @param json json
     * @param typeOfT 类型
     * @return Map/List
     * @param <T> 对象类型
     */
    public static <T> T fromJson(String json, Type typeOfT) {
        return GSON.fromJson(json, typeOfT);
    }
}
