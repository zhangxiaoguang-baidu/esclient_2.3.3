package com.meituan.trip.eswrapper.es.operation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.meituan.trip.eswrapper.base.ESConstants;
import com.meituan.trip.eswrapper.es.ESDoc;
import com.meituan.trip.eswrapper.es.ESRequest;
import com.meituan.trip.eswrapper.es.ESResponse;
import com.meituan.trip.eswrapper.es.connection.ESConnection;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    : 封装一些基本的es操作，无状态，不包含业务
 */
abstract class AbstractESOperations<T extends Collection<ESDoc>> implements InitializingBean, DisposableBean,
        ESOperations<T> {
    static final int DEFAULT_RETRY_COUNT = 2;
    private static final int DEFAULT_AGGREGATION_FIELD_COUNT = 10000;//

    Client esClient;
    private ESConnection esConnections;

    @Override
    public long getRequestTimeoutInMillis() {
        return 500L;
    }

    @Override
    public long getBulkIndexTimeoutInMillis() {
        return 1000L * 5;
    }

    /**
     * 批量更新文档的部分字段
     *
     * @param index     es的库
     * @param type      es的表
     * @param sourceMap 更新某个doc文档指定的字段
     */
    @Override
    public BulkResponse addOrUpdateIndexes(String index, String type, Map<String, Map<String, Object>> sourceMap) throws
                                                                                                                  Exception {
        BulkRequestBuilder bulkRequestBuilder = esClient.prepareBulk();

        for (String id : sourceMap.keySet()) {
            if (StringUtils.isBlank(id)) {
                continue;
            }
            Map<String, Object> mapDoc = sourceMap.get(id);
            if (MapUtils.isEmpty(mapDoc)) {
                continue;
            }
            final UpdateRequestBuilder updateRequestBuilder = esClient.prepareUpdate(index, type, id)
                                                                      .setDocAsUpsert(true).setDetectNoop(true)
                                                                      .setRetryOnConflict(DEFAULT_RETRY_COUNT)
                                                                      .setDoc(mapDoc);
            bulkRequestBuilder.add(updateRequestBuilder);
        }
        return bulkRequestBuilder.execute().actionGet(getBulkIndexTimeoutInMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 批量的添加或者更新索引，并记录成功或者失败日志
     *
     * @param index     es的库
     * @param type      es的表
     * @param esDocList 要增加或者更新的索引文档及其id
     */
    @Override
    public BulkResponse addOrUpdateIndexes(String index, String type, T esDocList) {
        BulkRequestBuilder bulkRequestBuilder = esClient.prepareBulk();
        for (ESDoc esDoc : esDocList) {
            final UpdateRequestBuilder updateRequestBuilder = esClient.prepareUpdate(index, type, esDoc.getId())
                                                                      .setDetectNoop(true).setDocAsUpsert(true)
                                                                      .setRetryOnConflict(DEFAULT_RETRY_COUNT)
                                                                      .setDoc(esDoc.getJsonDoc());
            bulkRequestBuilder.add(updateRequestBuilder);
        }
        return bulkRequestBuilder.execute().actionGet(getRequestTimeoutInMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @param index                       index
     * @param type                        type
     * @param esRequest                   esRequest
     * @param searchRequestBuilderHandler searchRequestBuilderHandler
     * @return
     */
    public ESResponse searchForIds(String index, String type, ESRequest esRequest,
                                   SearchRequestBuilderHandler searchRequestBuilderHandler) throws Exception {
        return searchForIds(index, type, esRequest, searchRequestBuilderHandler, null);
    }

    /**
     * 和 少一个参数的 searchForIds唯一的不同就是可以指定返回字段
     *
     * @param index                       index
     * @param type                        type
     * @param esRequest                   esRequest
     * @param searchRequestBuilderHandler searchRequestBuilderHandler
     * @param returnFields                返回字段，主要来自 core:field里边
     * @return
     */
    @Override
    public ESResponse searchForIds(String index, String type, ESRequest esRequest,
                                   SearchRequestBuilderHandler searchRequestBuilderHandler,
                                   Set<String> returnFields) throws Exception {
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type)
                                                            .setSearchType(SearchType.QUERY_THEN_FETCH)
                                                            .setFrom(esRequest.getOffset())
                                                            .setSize(esRequest.getLimit());

        if (CollectionUtils.isNotEmpty(returnFields)) {
            for (String retField : returnFields) {
                searchRequestBuilder.addField(retField);
            }
        }

        for (SortBuilder sortBuilder : esRequest.getSortBuilderList()) {
            searchRequestBuilder.addSort(sortBuilder);
        }
        searchRequestBuilder.setExplain(false);
        searchRequestBuilderHandler.setQueryOrFilter(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(getRequestTimeoutInMillis(),
                                                                                 TimeUnit.MILLISECONDS);
        SearchHits hits = searchResponse.getHits();
        List<String> ids = Lists.newArrayList();
        long total = hits == null ? 0 : hits.getTotalHits();
        if (hits == null || hits.getHits() == null || hits.getHits().length == 0) {
            return new ESResponse(ids, total);
        }
        for (SearchHit searchHit : hits.getHits()) {
            ids.add(searchHit.getId());
        }
        return new ESResponse(ids, total);
    }

    /**
     * 根据查询条件统计
     *
     * @param index
     * @param type
     * @param queryBuilder 查询条件
     * @param fields       要统计的fileds
     * @return
     */
    @Override
    public Map<String, Map<String, Long>> countByFilterBuilder(String index, String type, QueryBuilder queryBuilder,
                                                               Collection<String> fields) throws Exception {
        AggregationBuilder aggregationBuilder;
        if (queryBuilder != null) {
            aggregationBuilder = AggregationBuilders.filter(ESConstants.COUNT_PREFIX_STR).filter(queryBuilder);
        } else {
            aggregationBuilder = AggregationBuilders.global(ESConstants.COUNT_PREFIX_STR);
        }

        for (String field : fields) {
            aggregationBuilder.subAggregation(AggregationBuilders.terms(getCountFieldName(field)).field(field)
                                                                 .size(DEFAULT_AGGREGATION_FIELD_COUNT));
        }

        SearchResponse response = esClient.prepareSearch(index)
                                          .setTypes(type)
                                          .setSize(0)
                                          .setSearchType(SearchType.QUERY_THEN_FETCH)
                                          .addAggregation(aggregationBuilder)
                                          .setExplain(false)
                                          .execute()
                                          .actionGet(getRequestTimeoutInMillis(), TimeUnit.MILLISECONDS);

        Map<String, Map<String, Long>> result = Maps.newHashMap();
        final Aggregations aggregations = response.getAggregations();
        SingleBucketAggregation aggregation = aggregations.get(ESConstants.COUNT_PREFIX_STR);
        for (String field : fields) {
            LongTerms countTerms = aggregation.getAggregations().get(getCountFieldName(field));
            if (countTerms == null || CollectionUtils.isEmpty(countTerms.getBuckets())) {
                continue;
            }
            Map<String, Long> subResult = new HashMap<>(countTerms.getBuckets().size());
            for (Terms.Bucket bucket : countTerms.getBuckets()) {
                subResult.put(bucket.getKey() + "", bucket.getDocCount());
            }
            result.put(field, subResult);
        }
        return result;
    }

    private String getCountFieldName(String field) {
        return ESConstants.COUNT_FIELD_PREFIX_STR + field;
    }

    public void setEsConnections(ESConnection esConnections) {
        this.esConnections = esConnections;
    }

    @Override
    public void destroy() throws Exception {
        esConnections.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        esClient = esConnections.getClient();
    }
}
