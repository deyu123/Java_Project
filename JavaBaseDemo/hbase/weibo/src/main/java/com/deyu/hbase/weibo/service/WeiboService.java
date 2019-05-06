package com.deyu.hbase.weibo.service;

import com.deyu.hbase.weibo.dao.WeiboDao;
import com.deyu.hbase.weibo.util.Constant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiboService {

    private WeiboDao dao = new WeiboDao();

    public void createNameSpace(String nameSpace) throws IOException {
        dao.createNameSpace(nameSpace);
    }

    public void createTable(String tableName, String... families) throws IOException {
        dao.createTable(tableName, families);
    }

    public void createTable(String tableName, int versions, String... families) throws IOException {
        dao.createTable(tableName, versions, families);
    }

    public void publish(String userId, String content) throws IOException {

        //1.在weibo表中插入一条数据
        String rowKey = userId + "_" + System.currentTimeMillis();
        dao.putCell(Constant.TABLE_WEIBO, rowKey, Constant.WEIBO_FAMILY_DATA, Constant.WEIBO_COLUMN_CONTENT, content);

        //2.在relation表中获取所有的fansID
        String prefix = userId + ":followedby:";
        List<String> rowKeys = dao.getAllRowKeysByPrefix(Constant.TABLE_RELATION, prefix);

        if (rowKeys.size() <= 0) {
            return;
        }

        List<String> fansIds = new ArrayList<>();

        for (String key : rowKeys) {
            fansIds.add(key.split(":")[2]);
        }

        //3.将最新发布的weibo放入所有fans的inbox
        dao.putCell(Constant.TABLE_INBOX, fansIds, Constant.INBOX_FAMILY_DATA, userId, rowKey);
    }

    public void follow(String fans, String star) throws IOException {

        //1.往relation表中插入2条数据
        String time = System.currentTimeMillis() + "";
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        dao.putCell(Constant.TABLE_RELATION, rowKey1, Constant.RELATION_FAMILY_DATA, Constant.RELATION_COLUMN_TIME, time);
        dao.putCell(Constant.TABLE_RELATION, rowKey2, Constant.RELATION_FAMILY_DATA, Constant.RELATION_COLUMN_TIME, time);

        //2.获取star的近期weiboID
        String prefix = star;
        List<String> rowKeys = dao.getAllRowKeysByPrefix(Constant.TABLE_WEIBO, prefix);
        if (rowKeys.size() <= 0) {
            return;
        }

        int fromIndex = rowKeys.size() > 5 ? rowKeys.size() - 5 : 0;

        List<String> recentWeibos = rowKeys.subList(fromIndex, rowKeys.size());

        //3.往inbox表中插入star的最新weiboID
        for (String recentWeibo : recentWeibos) {
            dao.putCell(Constant.TABLE_INBOX, fans, Constant.INBOX_FAMILY_DATA, star, recentWeibo);
        }
    }

    public void unFollow(String fans, String star) throws IOException {

        //1.删除relation表中的两条数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        dao.deleteRow(Constant.TABLE_RELATION, rowKey1);
        dao.deleteRow(Constant.TABLE_RELATION, rowKey2);

        //2.删除inbox表中的fans对应的star列
        dao.deleteCell(Constant.TABLE_INBOX, fans, Constant.INBOX_FAMILY_DATA, star);

    }

    public List<String> getWeibosById(String star) throws IOException {
        List<String> list = dao.getCellsByPrefix(Constant.TABLE_WEIBO, star, Constant.WEIBO_FAMILY_DATA, Constant.WEIBO_COLUMN_CONTENT);
        return list;
    }

    public List<String> getAllRecentWeibos(String fans) throws IOException {

        //1.获取所有的weiboID
        List<String> list = dao.getCellsByRowKeyAndFamily(Constant.TABLE_INBOX, fans, Constant.INBOX_FAMILY_DATA);

        //2.根据weiboID去weibo表查content
        List<String> weibos = new ArrayList<>();
        for (String weiboId : list) {
            String content = dao.getCell(Constant.TABLE_WEIBO,weiboId,Constant.WEIBO_FAMILY_DATA,Constant.WEIBO_COLUMN_CONTENT);
            weibos.add(content);
        }
        return weibos;
    }
}
