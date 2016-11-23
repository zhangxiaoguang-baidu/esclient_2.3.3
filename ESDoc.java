package com.meituan.trip.eswrapper.es;

import java.util.Arrays;

/**
 * Author  : zhangxiaoguang
 * Date    : 16/11/21                          <br/>
 * Time    : 下午2:06                          <br/>
 * ---------------------------------------    <br/>
 * Desc    :es里的索引文档，分为id和主体两部分，主体是对象转成的json
 */
public class ESDoc {
    private String id;
    private byte[] jsonDoc;

    public ESDoc() {
    }

    public ESDoc(String id, byte[] jsonDoc) {
        this.id = id;
        this.jsonDoc = jsonDoc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public byte[] getJsonDoc() {
        return jsonDoc;
    }

    public void setJsonDoc(byte[] jsonDoc) {
        this.jsonDoc = jsonDoc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ESDoc)) {
            return false;
        }

        ESDoc ESDoc = (ESDoc) o;

        return id != null ? id.equals(ESDoc.id) : ESDoc.id == null && Arrays.equals(jsonDoc, ESDoc.jsonDoc);

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (jsonDoc != null ? Arrays.hashCode(jsonDoc) : 0);
        return result;
    }
}
