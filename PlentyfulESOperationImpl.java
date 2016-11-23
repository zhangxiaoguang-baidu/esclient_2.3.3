package com.meituan.trip.eswrapper.es.operation;

import com.meituan.trip.eswrapper.base.RetryUtil;
import com.meituan.trip.eswrapper.es.ESDoc;
import com.meituan.trip.eswrapper.es.ESRequest;
import com.meituan.trip.eswrapper.es.ESResponseMore;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Author  : zhangxiaoguang
 * Date    : 15/12/15
 * Time    : 下午9:38
 * ---------------------------------------
 * Desc    : 更加丰富的API实现
 */
public class PlentyfulESOperationImpl extends AbstractESOperations<List<ESDoc>> implements
        PlentyfulESOperations<List<ESDoc>> {
    private static final Logger log = LoggerFactory.getLogger(PlentyfulESOperationImpl.class);

    @Override
    public ESResponseMore searchMore(String index, String type, ESRequest esSearch,
                                     SearchRequestBuilderHandler searchRequestBuilderHandler) throws Exception {
        return searchMore(index, type, esSearch, searchRequestBuilderHandler, new HashSet<String>());
    }

    @Override
    public ESResponseMore searchMore(String index, String type, ESRequest esRequest,
                                     SearchRequestBuilderHandler searchRequestBuilderHandler,
                                     Set<String> obtainFields) throws Exception {
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type)
                                                            .setSearchType(SearchType.QUERY_THEN_FETCH)
                                                            .setFrom(esRequest.getOffset())
                                                            .setSize(esRequest.getLimit());
        for (SortBuilder sortBuilder : esRequest.getSortBuilderList()) {
            searchRequestBuilder.addSort(sortBuilder);
        }

        if (CollectionUtils.isNotEmpty(obtainFields)) {
            String[] fieldArray = obtainFields.toArray(new String[obtainFields.size()]);
            searchRequestBuilder.setFetchSource(fieldArray, null);
        }

        searchRequestBuilder.setExplain(false);
        searchRequestBuilderHandler.setQueryOrFilter(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet(getRequestTimeoutInMillis(),
                                                                                 TimeUnit.MILLISECONDS);
        SearchHits hits = searchResponse.getHits();
        ESResponseMore searchMore = new ESResponseMore();
        long total = hits == null ? 0 : hits.getTotalHits();
        searchMore.setTotal(total);
        if (hits == null || hits.getHits() == null || hits.getHits().length == 0) {
            return searchMore;
        }

        for (SearchHit searchHit : hits.getHits()) {
            searchMore.getFieldValue().add(searchHit.getSource());
        }
        return searchMore;
    }

    @Override
    public ESResponseMore searchMoreHighlight(String index, String type, ESRequest esSearch,
                                              Set<String> highlightFields,
                                              SearchRequestBuilderHandler searchRequestBuilderHandler,
                                              Set<String> obtainFields) throws Exception {
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type)
                                                            .setSearchType(SearchType.QUERY_THEN_FETCH)
                                                            .setFrom(esSearch.getOffset())
                                                            .setSize(esSearch.getLimit());
        if (CollectionUtils.isNotEmpty(highlightFields)) {
            for (String field : highlightFields) {
                searchRequestBuilder.addHighlightedField(field, 0, 1);
            }
        }
        for (SortBuilder sortBuilder : esSearch.getSortBuilderList()) {
            searchRequestBuilder.addSort(sortBuilder);
        }

        if (CollectionUtils.isNotEmpty(obtainFields)) {
            String[] fieldArray = obtainFields.toArray(new String[obtainFields.size()]);
            searchRequestBuilder.setFetchSource(fieldArray, null);
        }

        searchRequestBuilder.setExplain(false);
        searchRequestBuilderHandler.setQueryOrFilter(searchRequestBuilder);

        SearchResponse searchResponse =
                searchRequestBuilder.execute().actionGet(getRequestTimeoutInMillis(), TimeUnit.MILLISECONDS);
        SearchHits hits = searchResponse.getHits();
        ESResponseMore searchMore = new ESResponseMore();
        long total = hits == null ? 0 : hits.getTotalHits();
        searchMore.setTotal(total);
        if (hits == null || hits.getHits() == null || hits.getHits().length == 0) {
            return searchMore;
        }

        for (SearchHit searchHit : hits.getHits()) {
            Map<String, Object> fvMap = searchHit.getSource();
            if (MapUtils.isEmpty(fvMap)) {
                continue;
            }
            Map<String, HighlightField> map = searchHit.getHighlightFields();
            for (String hlight : highlightFields) {
                HighlightField highlightField = map.get(hlight);
                if (highlightField == null) {
                    continue;
                }
                Text[] texts = highlightField.getFragments();
                if (texts == null || texts.length == 0) {
                    continue;
                }
                String text = texts[0].string();
                String keyWord = text.substring(text.indexOf("<em>") + 4, text.length() - 5);
                fvMap.put(hlight, keyWord);
            }
            searchMore.getFieldValue().add(fvMap);
        }
        return searchMore;
    }

    /**
     * 创建索引,如果该索引存在则返回null
     *
     * @param index          index
     * @param type           type
     * @param indexAlias     indexAlias     如果为空则使用默认的别名
     * @param mappingBuilder mappingBuilder
     * @return
     * @throws Exception
     */
    @Override
    public CreateIndexResponse createIndex(final String index, final String type, final String indexAlias,
                                           final XContentBuilder mappingBuilder) throws Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<CreateIndexResponse>() {
            @Override
            public CreateIndexResponse execute() throws Exception {
                CreateIndexRequestBuilder builder = esClient.admin().indices().prepareCreate(index);
                if (StringUtils.isNotBlank(indexAlias)) {
                    builder.addAlias(new Alias(indexAlias));
                }
                return builder.addMapping(type, mappingBuilder).execute()
                              .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());
    }

    @Override
    public CreateIndexResponse createIndex(String index, String type, String indexAlias,
                                           XContentBuilder mappingBuilder,
                                           Map<String, Object> settingMap) throws Exception {
        CreateIndexResponse createIndex = createIndex(index, type, indexAlias, mappingBuilder);
        if (MapUtils.isEmpty(settingMap)) {
            return createIndex;
        }
        try {
            esClient.admin().indices().prepareUpdateSettings(index).setSettings(settingMap);
        } catch (Exception e) {
            log.error("", e);
        }
        return createIndex;
    }

    @Override
    public PutMappingResponse defineMapping(final String index, final String type,
                                            final XContentBuilder mappingBuilder) throws
                                                                                  Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<PutMappingResponse>() {
            @Override
            public PutMappingResponse execute() throws Exception {
                return esClient.admin().indices().preparePutMapping()
                               .setIndices(index)
                               .setType(type)
                               .setSource(mappingBuilder)
                               .execute()
                               .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());
    }

    @Override
    public IndicesAliasesResponse addAlias(final String index, final String alias) throws
                                                                                   Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<IndicesAliasesResponse>() {
            @Override
            public IndicesAliasesResponse execute() throws Exception {
                return esClient.admin().indices().prepareAliases()
                               .addAlias(index, alias)
                               .execute()
                               .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());
    }

    /**
     * 通过别名搜索索引
     *
     * @param aliases alias
     * @return
     * @throws Exception
     */
    @Override
    public GetAliasesResponse searchIndexByAlias(final String... aliases) throws Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<GetAliasesResponse>() {
            @Override
            public GetAliasesResponse execute() throws Exception {
                return esClient.admin().indices().prepareGetAliases(aliases)
                               .execute()
                               .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis() * 2);
    }

    @Override
    public boolean aliasExists(final String alias) throws Exception {
        Boolean result = RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<Boolean>() {
            @Override
            public Boolean execute() throws Exception {
                return esClient.admin().indices().prepareAliasesExist(alias)
                               .get(TimeValue.timeValueMillis(getRequestTimeoutInMillis() * 2)).exists();
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());

        return result != null && result;
    }

    @Override
    public boolean isIndexExists(final String indexName) throws Exception {
        Boolean result = RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<Boolean>() {
            @Override
            public Boolean execute() throws Exception {
                return esClient.admin().indices().prepareExists(indexName)
                               .get(TimeValue.timeValueMillis(getRequestTimeoutInMillis() * 2))
                               .isExists();
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());

        return result != null && result;
    }

    @Override
    public IndicesAliasesResponse replaceAliasAtomic(final String oldIndex, final String newIndex, final String alias)
            throws Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<IndicesAliasesResponse>() {
            @Override
            public IndicesAliasesResponse execute() throws Exception {
                return esClient.admin().indices().prepareAliases()
                               .addAliasAction(AliasAction.newRemoveAliasAction(oldIndex, alias))
                               .addAliasAction(AliasAction.newAddAliasAction(newIndex, alias))
                               .execute()
                               .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());
    }

    @Override
    public DeleteIndexResponse deleteIndex(final String index) throws Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<DeleteIndexResponse>() {
            @Override
            public DeleteIndexResponse execute() throws Exception {
                return esClient.admin().indices().prepareDelete(index)
                               .execute()
                               .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());
    }

    @Override
    public DeleteResponse deleteDoc(final String index, final String type, final String id) throws Exception {
        return RetryUtil.retryByCountWhenExceptionAndThrow(new RetryUtil.IExecutor<DeleteResponse>() {
            @Override
            public DeleteResponse execute() throws Exception {
                long version = esClient.prepareGet(index, type, id)
                                       .get(TimeValue.timeValueMillis(getRequestTimeoutInMillis() * 2))
                                       .getVersion();
                if (version <= 0L) {
                    throw new Exception("deleteDoc error ,id:" + id);
                }
                return esClient.prepareDelete(index, type, id)
                               .setVersion(version)
                               .execute()
                               .actionGet(getRequestTimeoutInMillis() * 2, TimeUnit.MILLISECONDS);
            }
        }, DEFAULT_RETRY_COUNT, log, getRequestTimeoutInMillis());
    }
}
