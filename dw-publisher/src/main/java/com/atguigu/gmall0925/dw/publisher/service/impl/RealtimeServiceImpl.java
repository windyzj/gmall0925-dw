package com.atguigu.gmall0925.dw.publisher.service.impl;

import com.atguigu.gmall0925.dw.constant.GmallConstants;
import com.atguigu.gmall0925.dw.publisher.service.RealtimeService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
}
