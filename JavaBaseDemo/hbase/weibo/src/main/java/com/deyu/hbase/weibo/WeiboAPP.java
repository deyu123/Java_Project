package com.deyu.hbase.weibo;

import com.deyu.hbase.weibo.controller.WeiboController;

import java.io.IOException;

public class WeiboAPP {
    private static WeiboController controller = new WeiboController();
    public static void main(String[] args) throws IOException {
        controller.init();
        controller.publish("1001","happy 5.1");
        controller.publish("1001","happy 5.2");
        controller.publish("1001","happy 5.3");
        controller.publish("1001","happy 5.4");
        controller.publish("1001","happy 5.5");
        controller.publish("1001","happy 5.6");
        controller.publish("1001","happy 5.7");
        controller.publish("1001","happy 5.8");

        controller.follow("1002","1001");
        controller.getWeibosById("1001");

        controller.publish("1003","unhappy 5.1");
        controller.publish("1003","unhappy 5.2");
        controller.publish("1003","unhappy 5.3");
        controller.publish("1003","unhappy 5.4");
        controller.publish("1003","unhappy 5.5");
        controller.publish("1003","unhappy 5.6");
        controller.publish("1003","unhappy 5.7");
        controller.publish("1003","unhappy 5.8");

        controller.follow("1002","1003");

        controller.getAllRecentWeibos("1002");

        controller.unFollow("1002","1001");

        controller.getAllRecentWeibos("1002");

    }
}
