package com.atguigu.gmall0925.dw.publisher.service.impl;

import com.atguigu.gmall0925.dw.constant.GmallConstants;
import com.atguigu.gmall0925.dw.publisher.service.RealtimeService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.SumAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RealtimeServiceImpl implements RealtimeService {

    @Autowired
    JestClient jestClient;



    @Override
    public int getDauTotal(String date) {
        int total=0;
        String query="{\n" +
                "  \"query\":{\n" +
                "      \"bool\": {\n" +
                "        \"filter\": {\n" +
                "           \"term\": {\n" +
                "             \"logDate\": \"2019-03-22\"\n" +
                "           }\n" +
                "        }\n" +
                "      }\n" +
                "  }\n" +
                "  , \"aggs\": {\n" +
                "    \"groupby_logHour\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"logHour.keyword\",\n" +
                "        \"size\": 24\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DAU).addType(GmallConstants.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            total=searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return total;
    }

    @Override
    public Map getDauHours(String date) {
        Map dauHoursMap=new HashMap(64);

        //过滤
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

         //聚合部分
        TermsBuilder groupbyLogHour = AggregationBuilders.terms("groupby_logHour").field("logHour.keyword").size(24);
        searchSourceBuilder.aggregation(groupbyLogHour);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DAU).addType(GmallConstants.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //获得聚合结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                dauHoursMap.put(bucket.getKey() ,bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dauHoursMap;
    }


    /***
     * 订单总金额
     * @param date
     * @return
     */
    public Double getOrderTotalAmount(String date){
        Double orderTotalAmount =0D;
     //过滤
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));

        searchSourceBuilder.query(boolQueryBuilder);
     //聚合
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        searchSourceBuilder.aggregation(sumBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_ORDER).addType(GmallConstants.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //取得聚合结果
            orderTotalAmount = searchResult.getAggregations().getSumAggregation("sum_totalAmount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orderTotalAmount;
    }

    /***
     * 求分时订单金额
     * @param date
     * @return
     */
    public Map<String,Double> getOrderTotalAmountHours(String date) {
        Map<String ,Double> map=new HashMap<>();
        //过滤
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        // 先做子聚合
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        //子聚合 放入 父聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24).subAggregation(sumBuilder);
        searchSourceBuilder.aggregation(termsBuilder);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_ORDER).addType(GmallConstants.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //先取分组
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String hour = bucket.getKey();
                //取各个分组的 sum聚合结果
                Double sum_totalamount = bucket.getSumAggregation("sum_totalamount").getSum();
                map.put(hour,sum_totalamount);
            }

        }catch  (IOException e) {
            e.printStackTrace();
        }

        return map;

    }

    /***
     * 根据 日期 关键字 聚合字段 查询购买明细，聚合结果
     * @param date
     * @param keyword
     * @param aggFieldName
     * @param aggSize
     * @return
     */
    public Map getSaleDetail(String date ,String keyword,String aggFieldName,int aggSize,int startPage,int pageSize ){
        Map saleDetailMap=new HashMap();

        if(aggSize==0){
            aggSize=1000;
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //日期过滤 关键字匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //按某个字段进行聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_" + aggFieldName).field(aggFieldName).size(aggSize);
        searchSourceBuilder.aggregation(termsBuilder);
        System.out.println(searchSourceBuilder.toString());

        searchSourceBuilder.size(pageSize);
        searchSourceBuilder.from((startPage-1)*pageSize);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_SALE).addType(GmallConstants.ES_DEFAULT_TYPE).build();
        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
              total = searchResult.getTotal();
            //获取明细数据
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
            List sourceList=new ArrayList();
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                HashMap source = hit.source;
                sourceList.add(source);
            }
            saleDetailMap.put("detail",sourceList);
            //获取聚合结果
            HashMap aggsMap=new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggFieldName).getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                aggsMap.put( bucket.getKey(),bucket.getCount());
            }
            saleDetailMap.put("aggs",aggsMap);

            saleDetailMap.put("total",total);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return saleDetailMap;
    }



}
