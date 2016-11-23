package com.meituan.trip.eswrapper.es;

import com.google.common.collect.Lists;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.List;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    :
 */
public class ESRequest {
    private int offset;
    private int limit;
    private List<SortBuilder> sortBuilderList = Lists.newArrayList();

    public ESRequest() {
    }

    public ESRequest(int limit, int offset, List<SortBuilder> sortBuilderList) {
        this.limit = limit;
        this.offset = offset;
        this.sortBuilderList = sortBuilderList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ESRequest)) {
            return false;
        }

        ESRequest esRequest = (ESRequest) o;

        if (offset != esRequest.offset) {
            return false;
        }
        if (limit != esRequest.limit) {
            return false;
        }

        return sortBuilderList != null && sortBuilderList.equals(esRequest.sortBuilderList);
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public List<SortBuilder> getSortBuilderList() {
        return sortBuilderList;
    }

    public void setSortBuilderList(List<SortBuilder> sortBuilderList) {
        this.sortBuilderList = sortBuilderList;
    }

    public ESRequest addSortBuilder(SortBuilder sortBuilder) {
        sortBuilderList.add(sortBuilder);
        return this;
    }

    @Override
    public int hashCode() {
        int result = sortBuilderList != null ? sortBuilderList.hashCode() : 0;
        result = 31 * result + offset;
        result = 31 * result + limit;
        return result;
    }
}
