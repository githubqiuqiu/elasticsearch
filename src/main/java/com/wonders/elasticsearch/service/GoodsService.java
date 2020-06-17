package com.wonders.elasticsearch.service;

import com.wonders.elasticsearch.entity.Goods;
import com.wonders.elasticsearch.repository.GoodsRepository;
import com.wonders.elasticsearch.util.ElasticsearchOperationUtils;
import com.wonders.elasticsearch.util.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author qiu
 * @date 2020-06-17 13:39
 */
@Service
public class GoodsService {

    @Autowired
    private GoodsRepository goodsRepository;

    @Autowired
    private HtmlParseUtil htmlParseUtil;

    @Autowired
    private ElasticsearchOperationUtils elasticsearchOperationUtils;

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;


    private static final String INDEX_NAME="jd_goods";


    public void initGoods(String keyword) throws Exception {
        for(int i=1;i<20;i+=2){
            List<Goods> list = htmlParseUtil.getGoodsListFromJd(keyword, i);
            //入库
            goodsRepository.saveAll(list);

            BulkResponse responses = elasticsearchOperationUtils.batchInsert(INDEX_NAME, list);
        }
    }


    public List<Map<String,Object>> searchList(String keyword,int pageNo,int pageSize) throws IOException {

        if(pageNo<1||StringUtils.isEmpty(keyword)){
            pageNo=1;
        }

        if(pageSize<1||StringUtils.isEmpty(pageSize)){
            pageSize=10;
        }

        //条件查询
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        //条件构造器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 因为在es的mapping中, title字段类型为text, 采用的又是默认的分词, 所以页面查询java, 是可以查到分词后的java单词的
        // 正常的开发中, 查询一般走的是BoolQueryBuilder
        //精准匹配
        //TermQueryBuilder title = QueryBuilders.termQuery("title", keyword);

        MatchQueryBuilder title = QueryBuilders.matchQuery("title", keyword);
        searchSourceBuilder.query(title);

        //高亮设置
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        // 支持多个高亮设置为true, 比如一段文字有多个java单词, 若设置false, 则只高亮第一个java单词
        highlightBuilder.requireFieldMatch(true);
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);

        //执行搜索
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);


        List<Map<String,Object>> mapList=new ArrayList<>();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            //解析高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            if(!StringUtils.isEmpty(highlightField)){
                Text[] fragments = highlightField.fragments();

                StringBuilder newTitle=new StringBuilder();
                for (Text fragment : fragments) {
                    newTitle.append(fragment);
                }

                //替换title为高亮title即可
                sourceAsMap.put("title",newTitle);
            }
            mapList.add(sourceAsMap);
        }

        return mapList;
    }

}
