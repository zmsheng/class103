package com.leyou.es;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.leyou.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ESManager {
    RestHighLevelClient client = null;

    Gson gson = new Gson();

    @Before
    public void init() throws Exception{
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9201, "http"),
                        new HttpHost("127.0.0.1", 9202, "http"),
                        new HttpHost("127.0.0.1", 9203, "http")));
    }

    @Test
    public void testDoc() throws Exception{
        Item item = new Item("1","小米9手机","手机","小米",1199.0,"q3311");
//        IndexRequest专门用来插入索引数据的对象
        IndexRequest request = new IndexRequest( "item","docs",item.getId());
//        把对象转成json字符串
//        String jsonString = JSON.toJSONString(item);  //fastjson转json的方式
        String jsonString = gson.toJson(item);   //gson转json的方式
        request.source(jsonString, XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);
    }
    @Test
    public void testdeleteDoc() throws Exception{
        DeleteRequest request = new DeleteRequest("item","docs","1");
        client.delete(request, RequestOptions.DEFAULT);
    }
    @Test
    public void testBulkAddDoc() throws Exception{
        List<Item> list = new ArrayList<>();
        list.add(new Item("1", "小米手机7", "手机", "小米", 3299.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("2", "坚果手机R1", "手机", "锤子", 3699.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("3", "华为META10", "手机", "华为", 4499.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("5", "荣耀V10", "手机", "华为", 2799.00,"http://image.leyou.com/13123.jpg"));

        BulkRequest request = new BulkRequest();
       /* for (Item item : list) {
            IndexRequest indexRequest = new IndexRequest( "item","docs",item.getId());
            String jsonString = gson.toJson(item);   //gson转json的方式
            indexRequest.source(jsonString, XContentType.JSON);
            request.add(indexRequest);
        }*/
        list.forEach(item->{  //流式编程
            IndexRequest indexRequest = new IndexRequest( "item","docs",item.getId());
            String jsonString = gson.toJson(item);   //gson转json的方式
            indexRequest.source(jsonString, XContentType.JSON);
            request.add(indexRequest);
        });
        client.bulk(request,RequestOptions.DEFAULT);
    }


    @Test
    public void testSearch() throws Exception{
//        构建一个用来查询的对象
        SearchRequest searchRequest = new SearchRequest("item").types("docs");
//        构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        查询所有
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//        term查询


//        searchSourceBuilder.fetchSource(new String[]{"id","title"},null);  //包含
//        searchSourceBuilder.fetchSource(null,new String[]{"id","title"});  //排除
//        searchSourceBuilder.query(QueryBuilders.termQuery("title","手机"));
//        searchSourceBuilder.postFilter(QueryBuilders.termQuery("brand","锤子"));

//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//        searchSourceBuilder.from(0);
//        searchSourceBuilder.size(20);
//
//        searchSourceBuilder.sort("price", SortOrder.DESC);

//        searchSourceBuilder.query(QueryBuilders.termQuery("title","小米"));
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.field("title");
//        highlightBuilder.preTags("<span style='color:red'>");
//        highlightBuilder.postTags("</span>");
////        "fields": {"title": {}},
////        "pre_tags": "<span style='color:red'>",
////                "post_tags": "</span>"
//        searchSourceBuilder.highlighter(highlightBuilder);


        searchSourceBuilder.aggregation(AggregationBuilders.terms("brandCount").field("brand"));


//     放入到searchRequest中
        searchRequest.source(searchSourceBuilder);
//        执行查询
        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Terms terms = aggregations.get("brandCount");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
//            bucket:{
//                "key": "华为",
//                        "doc_count": 2
//            }
            System.out.println( bucket.getKeyAsString()+":"+bucket.getDocCount());
        });

//        SearchHits responseHits = searchResponse.getHits();
//        System.out.println("总记录数是："+responseHits.getTotalHits());
//        SearchHit[] searchHits = responseHits.getHits();
        
        
        

//        for (SearchHit searchHit : searchHits) {
//
//           /* searchHit:{
//                "_index": "item",
//                    "_type": "docs",
//                    "_id": "4",
//                    "_score": 0.9529823,
//                    "_source": {
//                "id": "4",
//                        "title": "小米Mix2S",
//                        "category": "手机",
//                        "brand": "小米",
//                        "price": 4299,
//                        "images": "http://image.leyou.com/13123.jpg"
//            },
//                "highlight": {
//                "title": [
//                "<span style='color:red'>小米</span>Mix2S"
//          ]
//            }
//            }*/
//
////            把json字符串转成对象
//            String jsonString = searchHit.getSourceAsString();
//            Item item = gson.fromJson(jsonString, Item.class);
//
//            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
//            HighlightField highlightField = highlightFields.get("title");
//            Text[] fragments = highlightField.getFragments();
//            if(fragments!=null&&fragments.length>0){
//                String title = fragments[0].toString();
//                item.setTitle(title); //把item的title替换成高亮的数据
//            }
//
//
//            System.out.println(item);
//        }


    }



    @After
    public void end() throws Exception{
        client.close();
    }

}
