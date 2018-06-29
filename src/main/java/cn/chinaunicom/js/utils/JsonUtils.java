package cn.chinaunicom.js.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * @author : chris
 * 2018-06-29
 */
public class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 将对象转换成json字符串.
     *
     * @param data
     * @return
     */
    public static String objectToJson(Object data) {
        try {
            return MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将json结果集转化为对象
     *
     * @param jsonData
     * @param beanType
     * @param <T>
     * @return
     */
    public static <T> T jsonToPojo(String jsonData, Class<T> beanType) {
        try {
            return MAPPER.readValue(jsonData, beanType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> List<T> jsonToList(String jsonData, Class<T> beanType) {
        final JavaType javaType = MAPPER.getTypeFactory().constructParametricType(List.class, beanType);
        try {
            final List<T> t = (List<T>) MAPPER.readValue(jsonData, beanType);
            return t;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
