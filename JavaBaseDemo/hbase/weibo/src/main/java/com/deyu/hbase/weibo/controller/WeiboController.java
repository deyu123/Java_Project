package com.deyu.hbase.weibo.controller;

import com.deyu.hbase.weibo.service.WeiboService;
import com.deyu.hbase.weibo.util.Constant;

import java.io.IOException;
import java.util.List;

public class WeiboController {
    private WeiboService service = new WeiboService();

    public void init() throws IOException {

        //1) 创建命名空间以及表名的定义
        service.createNameSpace(Constant.NAMESPACE_WEIBO);

        //2) 创建微博内容表
        service.createTable(Constant.TABLE_WEIBO, Constant.WEIBO_FAMILY_DATA);

        //3) 创建用户关系表
        service.createTable(Constant.TABLE_RELATION, Constant.RELATION_FAMILY_DATA);

        //4) 创建用户微博内容接收邮件表
        service.createTable(Constant.TABLE_INBOX, 5, Constant.INBOX_FAMILY_DATA);
    }

    //5) 发布微博内容
    public void publish(String userId, String content) throws IOException {
        service.publish(userId, content);
    }

    //6) 添加关注用户
    public void follow(String fans, String star) throws IOException {
        service.follow(fans, star);

    }

    //7) 移除（取关）用户
    public void unFollow(String fans, String star) throws IOException {
        service.unFollow(fans, star);
    }

    //8) 获取关注的人的微博内容
    //1.获取某个被关注人的所有微博
    public void getWeibosById(String star) throws IOException {
        List<String> list = service.getWeibosById(star);
        for (String s : list) {
            System.out.println("weibo = " + s);
        }
    }

    //2.获取所有被关注人的近期微博
    public void getAllRecentWeibos(String fans) throws IOException {
        List<String> list = service.getAllRecentWeibos(fans);
        for (String s : list) {
            System.out.println("weibo = " + s);
        }
    }
}
