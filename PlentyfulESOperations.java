package com.meituan.trip.eswrapper.es.operation;

import com.meituan.trip.eswrapper.es.ESDoc;
import com.meituan.trip.eswrapper.es.ESRequest;
import com.meituan.trip.eswrapper.es.ESResponseMore;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Author  : zhangxiaoguang
 * Date    : 15/12/15
 * Time    : 下午9:36
 * ---------------------------------------
 * Desc    : 提供更加丰富的API接口
 */
public interface PlentyfulESOperations<T extends Collection<ESDoc>> extends ESOperations<T> {

    /**
     * 搜索更多的信息(包括解释信息和字段信息)
     *
     * @param index                       index
     * @param type                        type
     * @param esRequest                   esRequest
     * @param searchRequestBuilderHandler searchRequestBuilderHandler
     * @return
     */
    ESResponseMore searchMore(String index, String type, ESRequest esRequest,
                              SearchRequestBuilderHandler searchRequestBuilderHandler) throws Exception;

    /**
     * 搜索更多的信息(包括解释信息和字段信息)
     *
     * @param index                       index
     * @param type                        type
     * @param esRequest                   esRequest
     * @param searchRequestBuilderHandler searchRequestBuilderHandler
     * @param obtainFields                obtainFields
     * @return
     */
    ESResponseMore searchMore(String index, String type, ESRequest esRequest,
                              SearchRequestBuilderHandler searchRequestBuilderHandler, Set<String> obtainFields) throws
                                                                                                                 Exception;

    /**
     * 搜索更多的信息(包括解释信息和字段信息和高亮字段,该高亮字段只获取了命中的所有高亮字段的其中一个)
     *
     * @param index                       index
     * @param type                        type
     * @param esRequest                   esRequest
     * @param searchRequestBuilderHandler searchRequestBuilderHandler
     * @param obtainFields                obtainFields
     * @return
     */
    ESResponseMore searchMoreHighlight(String index, String type, ESRequest esRequest, Set<String> highlightFields,
                                       SearchRequestBuilderHandler searchRequestBuilderHandler,
                                       Set<String> obtainFields) throws Exception;

    /**
     * 别名是否存在
     *
     * @param alias 索引别名
     * @return
     * @throws Exception
     */
    boolean aliasExists(final String alias) throws Exception;

    /**
     * 索引是否存在
     *
     * @param indexName indexName
     * @return
     * @throws Exception
     */
    boolean isIndexExists(final String indexName) throws Exception;

    /**
     * 创建索引
     *
     * @param index          index
     * @param type           type
     * @param indexAlias     indexAlias
     * @param mappingBuilder mappingBuilder
     * @return
     * @throws Exception
     */
    CreateIndexResponse createIndex(final String index, final String type, final String indexAlias,
                                    final XContentBuilder mappingBuilder) throws Exception;

    /**
     * 创建索引并且更新索引settings,比如 interval
     *
     * @param index          index
     * @param type           type
     * @param indexAlias     indexAlias
     * @param mappingBuilder mappingBuilder
     * @param settingMap     如果是null或empty则不创建，否则更新settings,比如 refresh_interval
     * @return
     * @throws Exception
     */
    CreateIndexResponse createIndex(final String index, final String type, final String indexAlias,
                                    final XContentBuilder mappingBuilder,
                                    Map<String, Object> settingMap) throws Exception;

    /**
     * 定义索引mapping
     *
     * @param index          index
     * @param type           type
     * @param mappingBuilder mappingBuilder
     * @throws Exception
     */
    PutMappingResponse defineMapping(final String index, final String type, final XContentBuilder mappingBuilder)
            throws Exception;

    /**
     * 为某个索引增加别名
     *
     * @param index index
     * @param alias alias
     * @throws Exception
     */
    IndicesAliasesResponse addAlias(final String index, final String alias) throws Exception;

    /**
     * 通过别名搜索索引
     *
     * @param aliases aliases
     * @return
     * @throws Exception
     */
    GetAliasesResponse searchIndexByAlias(final String... aliases) throws Exception;

    /**
     * 原子性的切换别名
     *
     * @param oldIndex 别名刚开始指向的索引
     * @param newIndex 别名将要指向的新索引
     * @param alias    别名
     * @throws Exception
     */
    IndicesAliasesResponse replaceAliasAtomic(final String oldIndex, final String newIndex, final String alias)
            throws Exception;

    /**
     * 删除某个索引
     *
     * @param index index
     * @throws Exception
     */
    DeleteIndexResponse deleteIndex(final String index) throws Exception;

    /**
     * 删除某个索引的某个版本的某条文档
     *
     * @param index index
     * @param type  type
     * @param id    id
     * @return
     * @throws Exception
     */
    DeleteResponse deleteDoc(final String index, final String type, final String id) throws Exception;
}
