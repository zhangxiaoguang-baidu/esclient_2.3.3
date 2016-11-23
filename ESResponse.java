package com.meituan.trip.eswrapper.es;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    :
 */
public class ESResponse {
    private List<String> ids = Lists.newArrayList();
    private long total = 0L;

    public ESResponse() {
    }

    public ESResponse(List<String> ids, long total) {
        this.total = total;
        this.ids = ids;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
