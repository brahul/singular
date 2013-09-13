package com.singular.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 * Configuration related utility.
 *
 * @author Rahul Bhattacharjee
 */
public class Configuration extends Properties {

    public Configuration() {}

    public void putMap(String key , Map<String,String> value) {
        this.put(key,SingularUtil.serializerMap(value));
    }

    public void putList(String key , List<String> list) {
        this.put(key,SingularUtil.serializeList(list));
    }

    public Map<String,String> getMap(String key) {
        String val = (String) get(key);
        if(val == null)
            return null;
        return SingularUtil.deserializerMap(val);
    }

    public List<String> getList(String key) {
        String val = (String) get(key);
        if(val == null)
            return null;
        return SingularUtil.deserializeList(val);
    }

    /**
     * self test.
     *
     * @param argv
     * @throws Exception
     */

    /*
    public static void main(String [] argv) throws Exception {

        Map<String,String> value1 = new HashMap<String, String>();
        value1.put("one","one");
        value1.put("two","two");

        List<String> value2  = new ArrayList<String>();
        value2.add("one");
        value2.add("two");

        Configuration configuration = new Configuration();
        configuration.putMap("one", value1);
        configuration.putList("two",value2);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try{
            configuration.store(bout,"noting much.");
        }finally{
            bout.close();
        }

        System.out.println("Ser => " + new String(bout.toByteArray()));

        Configuration configuration1 = new Configuration();

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        try {
            configuration1.load(bin);
        }finally {
            bin.close();
        }

        System.out.println(configuration1.getMap("one"));
        System.out.println(configuration1.getList("two"));
    }
    */
}
