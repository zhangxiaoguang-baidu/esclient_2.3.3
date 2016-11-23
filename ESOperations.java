package com.meituan.trip.eswrapper.es.operation;

import com.meituan.trip.eswrapper.es.ESDoc;
import com.meituan.trip.eswrapper.es.ESRequest;
import com.meituan.trip.eswrapper.es.ESResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    : 封装一些基本的es操作，无状态，不包含业务
 */
public interface ESOperations<T extends Collection<ESDoc>> {

    /**
     * 获取查询的超时时间,单位是毫秒
     *
     * @return 毫秒超时时间
     */
    long getRequestTimeoutInMillis();

    /**
     * 获取查询的超时时间,单位是毫秒
     *
     * @return 毫秒超时时间
     */
    long getBulkIndexTimeoutInMillis();

    /**
     * 批量更新文档的部分字段
     *
     * @param index     es的库
     * @param type      es的表
     * @param sourceMap 更新某些doc文档指定的字段
     */
    BulkResponse addOrUpdateIndexes(String index, String type, Map<String, Map<String, Object>> sourceMap) throws
                                                                                                           Exception;

    /**
     * 批量的添加或者更新索引，并记录成功或者失败日志
     *
     * @param index es的库
     * @param type  es的表
     * @param esDoc 要增加或者更新的索引文档及其id
     */
    BulkResponse addOrUpdateIndexes(String index, String type, T esDoc) throws Exception;

    /**
     * <ul>
     * <li>通过filter搜索，可以缓存，速度快</li>
     * <li>通过query搜索</li>
     * <p/>
     * </ul>
     *
     * @param index      index
     * @param type       type
     * @param esRequest  esRequest
     * @param srbHandler searchRequestBuilderHandler
     * @return
     */
    ESResponse searchForIds(String index, String type, ESRequest esRequest, SearchRequestBuilderHandler srbHandler)
            throws Exception;

    /**
     * 和 少一个参数的 searchForIds唯一的不同就是可以指定返回字段
     *
     * @param index        index
     * @param type         type
     * @param esRequest    esRequest
     * @param srbHandler   searchRequestBuilderHandler
     * @param returnFields 返回字段
     * @return
     */
    ESResponse searchForIds(String index, String type, ESRequest esRequest, SearchRequestBuilderHandler srbHandler,
                            Set<String> returnFields) throws Exception;


    /**
     * 计数
     *
     * @param index        索引名称或者别名
     * @param type         type
     * @param queryBuilder queryBuilder
     * @param fields
     * @return
     */
    Map<String, Map<String, Long>> countByFilterBuilder(String index, String type, QueryBuilder queryBuilder,
                                                        Collection<String> fields) throws Exception;

    interface SearchRequestBuilderHandler {
        SearchRequestBuilder setQueryOrFilter(SearchRequestBuilder searchRequestBuilder);
    }
}
