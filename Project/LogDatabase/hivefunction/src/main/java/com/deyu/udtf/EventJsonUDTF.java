package com.deyu.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        //定义字段的集合
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("event_name");
        fieldNames.add("event_json");

        //定义字段类型的集合
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //核心逻辑处理
    @Override
    public void process(Object[] args) throws HiveException {

        //1.取出传入的参数:[{"ett":"1541146624055","en":"display","kv":{"copyright":"ESPN","content_provider":"CNN","extend2":"5","goodsid":"n4195","action":"2","extend1":"2","place":"3","showtype":"2","category":"72","newstype":"5"}},{"ett":"1541213331817","en":"loading","kv":{"extend2":"","loading_time":"15","action":"3","extend1":"","type1":"","type":"3","loading_way":"1"}},{"ett":"1541126195645","en":"ad","kv":{"entry":"3","show_style":"0","action":"2","detail":"325","source":"4","behavior":"2","content":"1","newstype":"5"}},{"ett":"1541202678812","en":"notification","kv":{"ap_time":"1541184614380","action":"3","type":"4","content":""}},{"ett":"1541194686688","en":"active_background","kv":{"active_source":"3"}}]
        String jsonStr = args[0].toString();

        try {
            //2.创建JSON数组
            JSONArray jsonArray = new JSONArray(jsonStr);

            //创建一个数组
            String[] strings = new String[2];

            //3.遍历JSON数组
            for (int i = 0; i < jsonArray.length(); i++) {

                //{"ett":"1541146624055","en":"display","kv":{"copyright":"ESPN","content_provider":"CNN","extend2":"5","goodsid":"n4195","action":"2","extend1":"2","place":"3","showtype":"2","category":"72","newstype":"5"}}
                JSONObject eventObj = jsonArray.getJSONObject(i);

                //取出事件名称
                strings[0] = eventObj.getString("en");
                strings[1] = eventObj.toString();

                //写出
                forward(strings);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void close() throws HiveException {

    }
}
