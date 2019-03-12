package com.atguigu.gmall0925.dw.interceptor;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor{
    Gson gson =null;

    @Override
    public void initialize() {
        gson=new Gson();
    }

    @Override
    public Event intercept(Event event) {
        // 从event 取出body  得到 type   ，根据type 加入不同的logType到header
        String jsonString = new String(event.getBody());

        HashMap hashMap = gson.fromJson(jsonString, HashMap.class);
        String type =(String) hashMap.get("type");
        //判断
        Map<String, String> headers = event.getHeaders();

        headers.put("logType",type);

       // event.setHeaders(headers);//?
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(  event);
        }

        return events;
    }

    @Override
    public void close() {

    }

    /**
     * 通过该静态内部类来创建自定义对象供flume使用，实现Interceptor.Builder接口，并实现其抽象方法
     */
    public static class Builder implements Interceptor.Builder {
        /**
         * 该方法主要用来返回创建的自定义类拦截器对象
         * @return
         */
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {
            //可以通过context得到 flume.conf中设置的参数 ，传递给Interceptor
        }
    }

}
