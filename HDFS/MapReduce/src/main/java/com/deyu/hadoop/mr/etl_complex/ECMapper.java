package com.deyu.hadoop.mr.etl_complex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ECMapper extends Mapper<LongWritable, Text, Text, LogBean> {

    private  LogBean logBean = new LogBean();
    private  Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parseLog(value.toString());
        context.getCounter("ETL Counter", "total").increment(1);
        if(logBean.isValid()){
            context.getCounter("ETL Counter", "true").increment(1);
            text.set(logBean.toString());
            context.write(text, logBean);
        }else {
            context.getCounter("ETL Counter", "false").increment(1);
        }

    }

    private void parseLog(String line) {
        // 1 截取
        String[] fields = line.split(" ");
        if (fields.length > 11) {
            // 2封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
            }else {
                logBean.setHttp_user_agent(fields[11]);
            }

            // 大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValid(false);
            }else{
                logBean.setValid(true);
            }
        }else {
            logBean.setValid(false);
        }
    }
}
