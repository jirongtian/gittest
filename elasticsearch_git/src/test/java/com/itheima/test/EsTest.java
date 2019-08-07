package com.itheima.test;

import com.alibaba.fastjson.JSON;
import com.itheima.pojo.Article;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EsTest {

    private TransportClient transportClient;

    @Before
    public void init() throws UnknownHostException {
        System.out.println("==init==");
        transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(
                        new InetSocketTransportAddress(
                                InetAddress.getByName("127.0.0.1"), 9300));
    }
    @After
    public void destory(){
        System.out.println("==destory==");
        transportClient.close();
    }

    /**
     *单独创建索引
     */
    @Test
    public void createIndex(){
        //单独创建索引库 需要admin()--indices()--prepareCreate("").get
        CreateIndexResponse response = transportClient.admin().indices().prepareCreate("blog2").get();
        System.out.println(response.isAcknowledged());
        System.out.println(response.isShardsAcked());

    }

    /**
     *删除索引库
     */
    @Test
    public void deleIndex(){
        DeleteIndexResponse response = transportClient.admin().indices().prepareDelete("blog2").get();
        System.out.println(response.isAcknowledged());

    }

    /**
     *设置mappings
     */
    @Test
    public void createMappings() throws IOException, ExecutionException, InterruptedException {
        //设置mappings 需要先创建索引 先判断
        IndicesExistsResponse existsResponse = transportClient.admin().indices().prepareExists("blog2").get();
       if(!existsResponse.isExists()){
           CreateIndexResponse response = transportClient.admin().indices().prepareCreate("blog2").get();
           System.out.println(response.isAcknowledged());
       }
        /**
         *
         * 格式：
         *  "mappings" :
         *  从这里开始
         *   {
         *      "article" : {
         *          "properties" : {
         *              "id" : { "type" : "long", "store":"yes" },
         *              "content" : { "type" : "string" , "store":"yes" , "analyzer":"ik_smart"},
         *              "title" : { "type" : "string", "store":"yes" , "analyzer":"ik_smart" }
         *           }
         *       }
         *   }
         */
        Map mappings = new HashMap();
        Map article = new HashMap();
        Map properties = new HashMap();

        Map id = new HashMap();
        id.put("type", "long");
        id.put("store", "yes");

        Map content = new HashMap();
        content.put("type", "string");
        content.put("store", "yes");
        content.put("analyzer", "ik_smart");

        Map title = new HashMap();
        title.put("type", "string");
        title.put("store", "yes");
        title.put("analyzer", "ik_smart");

        mappings.put("article", article);
        article.put("properties", properties);
        properties.put("id", id);
        properties.put("content", content);
        properties.put("title", title);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type","long").field("store","yes")
                .endObject()
                .startObject("content")
                .field("type","string").field("store","yes").field("analyzer", "ik_smart")
                .endObject()
                .startObject("title")
                .field("type","string").field("store","yes").field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        //通过XContentBuilder的方式设置mappings
        //2.设置mappings 设置索引库，设置type
        //PutMappingRequest request = Requests.putMappingRequest("blog2").type("article").source(builder);
        //也可以采用哈希map进行设置
        PutMappingRequest request = Requests.putMappingRequest("blog2").type("article").source(mappings);
        //3.通过putMapping将mappings设置给索引库
        PutMappingResponse response = transportClient.admin().indices().putMapping(request).get();
        System.out.println(response.isAcknowledged());


    }

    /**
     * 创建文档
     */
    @Test
    public void createDocument() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("id",1L )
                .field("title", "ElasticSearch是一个基于Lucene的搜索服务器")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是 用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能 够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();
        IndexResponse response = transportClient.prepareIndex("blog2", "article").setSource(builder).get();
        System.out.println(response.status());
    }
    /**
     * 通过object创建document对象到索引库
     */
    @Test
    public void createDocumentByObject(){
        Article article = new Article();
        article.setId(2L);
        article.setTitle("测试对象新增");
        article.setContent("测试对象内容");  //需要json序列化
        String jsonString = JSON.toJSONString(article);  //通过fastjson将对象转字符串

        /*Article article1 = JSON.parseObject(jsonString, Article.class);
        List<Article> articles = JSON.parseArray(jsonString, Article.class);*/

        //json字符串设置为source，参数2是类型
        IndexResponse response = transportClient.prepareIndex("blog2", "article",article.getId() + "").setSource(jsonString, XContentType.JSON).get();
        System.out.println(response.status());
    }
    /**
     * 修改文档
     */
    @Test
    public void updateDocument() throws ExecutionException, InterruptedException {
        Article article = new Article();
        article.setId(2l);
        article.setTitle("再次修改搜索工作其实很快乐");
        article.setContent("修改我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

        String jsonString = JSON.toJSONString(article);

        //采用prepareUpdate,注意是setDocument
        transportClient.prepareUpdate("blog2", "article",article.getId() + "").setDoc(jsonString,XContentType.JSON).get();

        //采用update
        //transportClient.update(new UpdateRequest("blog2", "article", article.getId()+"").doc(jsonString,XContentType.JSON)).get();
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteDocument(){
        DeleteResponse response = transportClient.prepareDelete("blog2", "article", "AWxkoiNqnxIbUCc5sFki").get();
        System.out.println(response.status());
    }

    /**
     * 批量添加数据
     * @throws UnknownHostException
     */
    @Test
    public void addData() throws UnknownHostException {

        for (int i = 0; i < 100; i++) {
            Article article = new Article();
            article.setId((long)i);
            article.setTitle(i + "好的修改搜索工作其实很快乐");
            article.setContent(i+"我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

            transportClient.prepareIndex("blog2", "article",article.getId() + "").setSource(JSON.toJSONString(article), XContentType.JSON).get();
        }
    }
    /**
     * 分页查询测试
     */
    @Test
    public void searchPage() {
        //查询全部matchAllQuery
        SearchRequestBuilder builder = transportClient.prepareSearch("blog2").setTypes("article").setQuery(QueryBuilders.matchAllQuery());
        //起始记录数 from = (pageNo-1)*pageSize  pageSize 每页记录数
        builder.setFrom(0).setSize(5);
        //设置排序的域  参数1:域名  参数2:升降序
        builder.addSort("id", SortOrder.ASC);
        SearchResponse response = builder.get();//builder设置分页参数后，才通过get发送请求
        SearchHits hits = response.getHits();
        System.out.println("记录数="+hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


}
