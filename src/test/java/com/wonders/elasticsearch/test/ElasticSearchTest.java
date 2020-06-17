package com.wonders.elasticsearch.test;

import com.alibaba.fastjson.JSON;
import com.wonders.elasticsearch.entity.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.swing.text.Highlighter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author qiu
 * @date 2020-06-16 17:29
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticSearchTest {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    private static final String INDEX_NAME="javatest";

    /**
     * elasticsearch 7.6.1 api测试
     *
     */

    //创建索引
    @Test
    public void createIndex() throws IOException {
        //1.new一个创建索引请求  相当于 PUT /javatest
        CreateIndexRequest javaestest = new CreateIndexRequest(INDEX_NAME);
        //2.客户端执行创建请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(javaestest, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);

    }


    //获取索引
    @Test
    public void getIndex() throws IOException {
        //1.new一个获取索引请求  相当于 GET /javatest
        GetIndexRequest javaestest = new GetIndexRequest(INDEX_NAME);
        boolean exists = restHighLevelClient.indices().exists(javaestest, RequestOptions.DEFAULT);
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(javaestest, RequestOptions.DEFAULT);

        System.out.println(exists);
        System.out.println(getIndexResponse);
    }

    //删除索引
    @Test
    public void deleteIndex() throws IOException {
        //1.new一个删除索引请求 相当于 DELETE /javatest
        DeleteIndexRequest javaestest = new DeleteIndexRequest(INDEX_NAME);
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(javaestest, RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse.isAcknowledged());
    }


    //添加文档 相当于添加数据
    @Test
    public void  addDocument() throws IOException {
        //1.创建文档对象
        User user = new User();
        user.setUsername("公孙离");
        user.setSex("女");
        user.setAge(19);

        //2.new一个创建请求
        IndexRequest request = new IndexRequest(INDEX_NAME);

        //3.创建规则 不设置id就是默认id 填值就是自定义id PUT /javatest/_doc/xxxx
        //request.id();

        //4.把数据放入请求 需要json格式
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //5.客户端发送请求
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(index.toString());

    }


    //获取文档
    @Test
    public void getDocument() throws IOException {
        //1.new一个获取请求
        GetRequest request=new GetRequest(INDEX_NAME,"Q-qkvHIBMSJRJJJYtV0x");

        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);

        //存在获取文档信息
        if(exists){
            GetResponse documentFields = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            System.out.println(documentFields.getSourceAsString());
            System.out.println(documentFields.toString());
        }
    }

    //更新文档
    @Test
    public void updateDocument() throws IOException {
        //1.创建文档对象
        User user = new User();
        user.setUsername("孙尚香");
        user.setAge(19);
        user.setSex("女");

        //2.new一个修改请求
        UpdateRequest request = new UpdateRequest(INDEX_NAME,"Q-qkvHIBMSJRJJJYtV0x");

        //3.把数据放入请求 需要json格式
        request.doc(JSON.toJSONString(user),XContentType.JSON);

        //4.客户端发送请求
        UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);

        System.out.println(updateResponse.toString());
    }


    //删除文档记录
    @Test
    public void deleteDocument() throws IOException {
        //1.new一个删除请求
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_NAME,"Q-qkvHIBMSJRJJJYtV0x");
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.toString());
    }

    //批量插入数据
    @Test
    public void batchAddDocument() throws IOException {
        //1.new一个批量处理请求
        BulkRequest bulkRequest = new BulkRequest();

        List<User> list=new ArrayList();
        list.add(new User("马可波罗","男",23));
        list.add(new User("蒙犽","男",12));
        list.add(new User("公孙离","女",18));
        list.add(new User("孙尚香","女",19));

        for (User user : list) {
            IndexRequest request = new IndexRequest(INDEX_NAME);
            request.source(JSON.toJSONString(user),XContentType.JSON);

            bulkRequest.add(request);
        }

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        //是否失败
        System.out.println(response.hasFailures());
    }

    //查询
    @Test
    public void searchData() throws IOException {
        //1.new一个查询请求
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);

        //2.构建条件构造器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /**
         *
         * HighlightBuilder 高亮构造器
         * 查询条件  使用QueryBuilders 构建查询条件
         * QueryBuilders.termQuery() 精确查询
         * QueryBuilders.matchQuery() 匹配查询
         * QueryBuilders.matchAllQuery() 匹配所有
         *
         */

        //构建查询条件
        //MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "公孙罗");
        //TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "公孙离");
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();


        //把查询条件 添加到查询构造器
        searchSourceBuilder.query(matchAllQueryBuilder);

        //构建分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(3);

        searchRequest.source(searchSourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


}
