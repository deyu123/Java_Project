package com.deyu.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

// 用户hive 的自定义函数
public class Lower extends UDF {
    public String evaluate (final String s) {

        if (s == null) {
            return null;
        }

        return s.toLowerCase();
    }

}
