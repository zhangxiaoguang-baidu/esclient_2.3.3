package com.meituan.trip.eswrapper.es;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    :
 */
public class ESResponseMore {
    private List<Map<String, Object>> fieldValue = Lists.newArrayList();
    private long total = 0L;

    public ESResponseMore(List<Map<String, Object>> fieldValue, long total) {
        this.fieldValue = fieldValue;
        this.total = total;
    }

    public ESResponseMore() {
    }

    public List<Map<String, Object>> getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(List<Map<String, Object>> fieldValue) {
        this.fieldValue = fieldValue;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
