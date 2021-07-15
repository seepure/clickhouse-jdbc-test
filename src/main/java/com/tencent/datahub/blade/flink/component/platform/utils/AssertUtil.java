package com.tencent.datahub.blade.flink.component.platform.utils;

public class AssertUtil {

    public static void assertTrue(boolean expression, String msg) {
        if (!expression) {
            throw new IllegalArgumentException(msg);
        }
    }
}
