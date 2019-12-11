package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;
import java.util.List;

public class PageModal<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<T> data;

    private long size;

    private long page;

    private long totalSize;

    public PageModal(List<T> data) {
        this(data, data.size(), -1, -1);
    }

    public PageModal(List<T> data, long size, long page) {
        this(data, size, page, -1);
    }

    public PageModal(List<T> data, long size, long page, long totalSize) {
        this.data = data;
        this.size = size;
        this.page = page;
        this.totalSize = totalSize;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getPage() {
        return page;
    }

    public void setPage(long page) {
        this.page = page;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }
}
