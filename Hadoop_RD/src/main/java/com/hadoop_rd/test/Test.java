package com.hadoop_rd.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        List list = new ArrayList();
        list.add(3);
        list.add("t");
        list.add("ok");
        System.out.println(list.toString());
        String a = list.toString();
        String[] b = a.split(",");
        ArrayList ll = new ArrayList(Arrays.asList(b));

    }
}
