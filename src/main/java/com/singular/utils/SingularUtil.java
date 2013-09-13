package com.singular.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils for singular.
 *
 * @author Rahul Bhattacharjee
 */
public class SingularUtil {

    private static final String DELIMITER = ":";
    private static final String ENTRY_DELIMITER = "=";

    public static String serializerMap(Map<String,String> map) {
        StringBuilder builder = new StringBuilder();

        for(Map.Entry<String,String> entry : map.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            builder.append(key + ENTRY_DELIMITER + val);
            builder.append(DELIMITER);
        }
        return builder.toString();
    }

    public static Map<String,String> deserializerMap(String serializedMap) {
        String [] entries = serializedMap.split(DELIMITER);
        Map<String,String> data = new HashMap<String, String>();
        for(String entry : entries) {
            String [] splits = entry.split("=");
            if(splits.length == 2) {
                String key = splits[0];
                String val = splits[1];
                data.put(key,val);
            }
        }
        return data;
    }

    public static String serializeList(List<String> data ) {
        StringBuilder builder = new StringBuilder();
        for(String entry : data) {
            builder.append(entry);
            builder.append(DELIMITER);
        }
        return builder.toString();
    }

    public static List<String> deserializeList(String data) {
        List<String> entryList = new ArrayList<String>();
        String [] splits = data.split(DELIMITER);
        for(String split : splits) {
            entryList.add(split);
        }
        return entryList;
    }
}
