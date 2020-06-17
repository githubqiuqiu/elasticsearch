package com.wonders.elasticsearch.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author qiu
 * @date 2020-06-17 14:08
 */
@Component
public class ElasticsearchOperationUtils {
    private Logger log= LoggerFactory.getLogger(ElasticsearchOperationUtils.class);

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    public String insertOne(String indexName, String type, Map<String, Object> data) {
        return this.insertOneForResponse(indexName, type, data).getId();
    }

    private IndexResponse insertOneForResponse(String indexName, String type, Map<String, Object> data) {
        IndexResponse indexResponse = null;
        try {
            IndexRequest indexRequest = this.getIndexRequest(indexName, type);
            indexRequest.source(data);
            indexResponse = this.restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("{} 插入数据失败", indexName, e);
        }
        return indexResponse;
    }

    private IndexRequest getIndexRequest(String indexName, String type) {
        IndexRequest indexRequest = new IndexRequest(indexName);
        if (StringUtils.isNotBlank(type)) {
            indexRequest.type(type);
        }
        return indexRequest;
    }

    public boolean insertOneSync(String indexName, String type, Map<String, Object> data, long overTime) {
        Callable<Boolean> callable = () -> {
            this.insertOneForResponse(indexName, type, data);
            return true;
        };
        return this.getTimeOut(callable, overTime);
    }

    private boolean getTimeOut(Callable<?> task, long timeOut) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        FutureTask<?> futureTask = (FutureTask) executorService.submit(task);
        executorService.execute(futureTask);
        try {
            futureTask.get(timeOut, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException var7) {
            var7.printStackTrace();
        }
        return false;
    }

    public boolean insertOneASync(String indexName, String type, Map<String, Object> data) {
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                log.error("异步单条插入成功");
            }

            @Override
            public void onFailure(Exception e) {
                log.error("异步单条插入失败", e);
            }
        };
        IndexRequest indexRequest = this.getIndexRequest(indexName, type);
        indexRequest.source(data);
        this.restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
        return true;
    }

    /* ---------------------------------------------------------------------------------------------------- */
    /* 批量操作 */
    /* ---------------------------------------------------------------------------------------------------- */

    public boolean insertBulkSync(String indexName, String type, List<Map<String, Object>> data) {
        this.getBulkInsertSync(indexName, type, data);
        return true;
    }

    private void getBulkInsertSync(String indexName, String type, List<Map<String, Object>> data) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            for (Map<String, Object> datum : data) {
                Map<String, Object> map = (Map) datum;
                IndexRequest indexRequest = this.getIndexRequest(indexName, type);
                indexRequest.source(map);
                bulkRequest.add(indexRequest);
            }
            this.restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException var8) {
            log.error("{} 批量插入失败", indexName, var8);
        }
    }

    public boolean insertBulkSyncData(String indexName, String type, List<?> data, String idFieldName) {
        this.getBulkInsertSyncData(indexName, type, data, idFieldName);
        return true;
    }

    private void getBulkInsertSyncData(String indexName, String type, List<?> dataList, String idFieldName) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            dataList.forEach((data) -> {
                UpdateRequest request = this.getUpdateRequest(indexName, type);
                try {
                    Object o = FieldUtils.readField(data, idFieldName, true);
                    request.id(String.valueOf(o));
                } catch (IllegalAccessException var8) {
                    var8.printStackTrace();
                }
                request.doc(JSON.toJSONString(data), XContentType.JSON);
                request.upsert(JSON.toJSONString(data), XContentType.JSON);
                bulkRequest.add(request);
            });
            this.restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException var6) {
            log.error("{} 批量插入失败", indexName, var6);
        }
    }

    public boolean insertBulkSync(String indexName, String type, List<Map<String, Object>> data, long overTime) {
        Callable<Boolean> callable = () -> {
            this.getBulkInsertSync(indexName, type, data);
            return true;
        };
        return this.getTimeOut(callable, overTime);
    }

    public boolean insertBulkASync(String indexName, String type, List<Map<String, Object>> data) {
        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {
                log.error("异步批量插入失败", e);
            }
        };
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> datum : data) {
            Map<String, Object> map = (Map) datum;
            IndexRequest indexRequest = this.getIndexRequest(indexName, type);
            indexRequest.source(map);
            bulkRequest.add(indexRequest);
        }
        this.restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
        return true;
    }

    /* ---------------------------------------------------------------------------------------------------- */

    public boolean upsertOneSyncById(String indexName, String type, Map<String, Object> data, String id) {
        this.upsertOneForResponse(indexName, type, data, id);
        return true;
    }

    private UpdateResponse upsertOneForResponse(String indexName, String type, Map<String, Object> data, String id) {
        UpdateResponse response = null;
        try {
            UpdateRequest request = this.getUpdateRequest(indexName, type);
            request.id(id);
            request.doc(data);
            request.upsert(data);
            response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        } catch (IOException var7) {
            log.error("{} 根据ID：{} 更新数据失败", indexName, id);
            var7.printStackTrace();
        }
        return response;
    }

    private UpdateRequest getUpdateRequest(String indexName, String type) {
        UpdateRequest request = new UpdateRequest();
        request.index(indexName);
        if (StringUtils.isNotBlank(type)) {
            // 7.x之前版本需要指定type, 而7.x版本type统一默认为_doc
            request.type(type);
        }
        return request;
    }

    public boolean upsertOneSyncById(String indexName, String type, Object data, String id) {
        this.upsertOneForResponse(indexName, type, data, id);
        return true;
    }

    private UpdateResponse upsertOneForResponse(String indexName, String type, Object data, String id) {
        UpdateResponse response = null;
        try {
            UpdateRequest request = this.getUpdateRequest(indexName, type);
            request.id(id);
            request.doc(JSON.toJSONString(data), XContentType.JSON);
            request.upsert(JSON.toJSONString(data), XContentType.JSON);
            response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        } catch (IOException var7) {
            log.error("{} 根据ID：{} 更新数据失败", indexName, id);
            var7.printStackTrace();
        }
        return response;
    }

    public boolean upsertOneSyncById(String indexName, String type, Map<String, Object> data, String id, long overTime) {
        Callable<Boolean> callable = () -> {
            this.upsertOneForResponse(indexName, type, data, id);
            return null;
        };
        return this.getTimeOut(callable, overTime);
    }

    public boolean upsertOneForAsyncResponse(String indexName, String type, Map<String, Object> data, String id) {
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                log.info("异步修改执行成功");
            }

            @Override
            public void onFailure(Exception e) {
                log.error("{} 异步修改执行失败", indexName, e);
            }
        };
        UpdateRequest request = this.getUpdateRequest(indexName, type);
        request.id(id);
        request.doc(data);
        request.upsert(data);
        this.restHighLevelClient.updateAsync(request, RequestOptions.DEFAULT, listener);
        return true;
    }


    public  BulkResponse batchInsert(String indexName,List list) throws Exception {
        //1.new一个批量处理请求
        BulkRequest bulkRequest = new BulkRequest();

        list.forEach(e->{
            IndexRequest request = new IndexRequest(indexName);
            request.source(JSON.toJSONString(e),XContentType.JSON);
            bulkRequest.add(request);
        });

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return response;
    }

}
