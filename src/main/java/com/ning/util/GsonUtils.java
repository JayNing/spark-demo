package com.ning.util;

import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: GsonUtils
 * Description:
 * date: 2020/9/17 15:36
 *
 * @author ningjianjian
 */
public class GsonUtils {
    private static final JsonParser jsonParser = new JsonParser();
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("a", "a");
        map.put("b", "b");
        map.put("c", "c");
        List list = new ArrayList();
        list.add("a");
        list.add("b");
        list.add("c");
        String str = "{\"age\":{\"zh\":\"11\",\"en\":12}}";
//        System.out.println(GsonUtils.toJsonArray(list));
        JsonObject jsonObject = toJsonObject(str);
        System.out.println(jsonObject.getAsJsonObject("age").get("zh").toString());
    }

    /**
     * Json字符串转成Bean
     *
     * @param jsonString
     * @param cls
     * @param <T>
     * @return
     */
    public static <T> T toBean(String jsonString, Class<T> cls) {
        T t = gson.fromJson(jsonString, cls);
        return t;
    }

    /**
     * Json字符串转成List
     *
     * @param jsonString
     * @param cls
     * @param <T>
     * @return
     */
    public static <T> List<T> toList(String jsonString, Class<T> cls) {
        List<T> list = new ArrayList<T>();
        JsonArray array = new JsonParser().parse(jsonString).getAsJsonArray();
        for (final JsonElement elem : array) {
            list.add(gson.fromJson(elem, cls));
        }
        return list;
    }

    /**
     * Json字符串转成Map
     *
     * @param jsonString
     * @param <T>
     * @return
     */
    public static <T> Map<String, T> toMap(String jsonString) {
        Map<String, T> map = gson.fromJson(jsonString, new TypeToken<Map<String, T>>() {}.getType());
        return map;
    }

    /**
     * Json字符串转成ListMap
     *
     * @param jsonString
     * @param <T>
     * @return
     */
    public static <T> List<Map<String, T>> toListMaps(String jsonString) {
        List<Map<String, T>> list = gson.fromJson(jsonString, new TypeToken<List<Map<String, T>>>() {}.getType());
        return list;
    }

    /**
     * Json字符串转成JsonObject
     *
     * @param jsonString
     * @return
     */
    public static JsonObject toJsonObject(String jsonString) {
        JsonObject jsonObject = jsonParser.parse(jsonString).getAsJsonObject();
        return jsonObject;
    }

    /**
     * Json字符串转成JsonArray
     *
     * @param jsonString
     * @return
     */
    public static JsonArray toJsonArray(String jsonString) {
        JsonArray jsonArray = jsonParser.parse(jsonString).getAsJsonArray();
        return jsonArray;
    }

    /**
     * 将Object对象转成json字符串
     *
     * @param object
     * @return
     */
    public static String toJsonString(Object object) {
        String jsonString = gson.toJson(object);
        return jsonString;
    }

    /**
     * 将Object对象转成JsonObject
     *
     * @param object
     * @return
     */
    public static JsonObject toJsonObject(Object object) {
        JsonObject jsonObject = gson.toJsonTree(object).getAsJsonObject();
        return jsonObject;
    }

    /**
     * 将List对象转成JsonArray
     *
     * @param list
     * @param <T>
     * @return
     */
    public static <T> JsonArray toJsonArray(List<T> list) {
        JsonArray jsonArray = gson.toJsonTree(list, new TypeToken<List<T>>() {}.getType()).getAsJsonArray();
        return jsonArray;
    }

    /**
     * 将JsonObject对象转成Bean
     *
     * @param jsonObject
     * @param cls
     * @param <T>
     * @return
     */
    public static <T> T toBean(JsonObject jsonObject, Class<T> cls) {
        T t = gson.fromJson(jsonObject, cls);
        return t;
    }

    /**
     * 将JsonArray转成List
     *
     * @param jsonArray
     * @param cls
     * @param <T>
     * @return
     */
    public static <T> List<T> toList(JsonArray jsonArray, Class<T> cls) {
        List<T> list = gson.fromJson(jsonArray, new TypeToken<List<T>>() {}.getType());
        return list;
    }

    /**
     * 将JsonObject转成Map
     *
     * @param jsonObject
     * @param cls
     * @param <T>
     * @return
     */
    public static <T> Map<String, T> toMap(JsonObject jsonObject, Class<T> cls) {
        Map<String, T> map = gson.fromJson(jsonObject, new TypeToken<Map<String, T>>() {}.getType());
        return map;
    }

    /**
     * json字符串转成泛型bean并指定时间格式转换
     *
     * @param jsonString
     * @param cls
     * @param pattern
     * @param <T>
     * @return
     */
    public static <T> T toBeanConvertDate(String jsonString, Class<T> cls, String pattern) {
        Gson gson = new Gson();
        if (StringUtils.isNotEmpty(pattern)) {
            gson = new GsonBuilder().setDateFormat(pattern).create();
        }
        T t = gson.fromJson(jsonString, cls);
        return t;
    }

    /**
     * 使用GSON格式化输出json字符串
     *
     * @param json
     * @return
     */
    public static String toPrettyFormat(String json) {
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(json).getAsJsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(jsonObject);
    }

    /**
     * 判断JsonArray是否为空
     *
     * @param jsonArray
     * @return
     */
    public static boolean isEmpty(JsonArray jsonArray) {
        return jsonArray == null || jsonArray.size() == 0;
    }

    /**
     * 获取jsonElement的字符串形式
     *
     * @param jsonElement
     * @return
     */
    public static String getString(JsonElement jsonElement) {
        if (jsonElement == null || jsonElement.isJsonNull()) {
            return StringUtils.EMPTY;
        } else if (jsonElement.isJsonPrimitive()) {
            return jsonElement.getAsString();
        } else {
            return jsonElement.toString();
        }
    }
}
