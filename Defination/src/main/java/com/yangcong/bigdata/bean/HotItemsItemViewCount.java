package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-01-04 23:35
 */
public class HotItemsItemViewCount implements Serializable {
    private static final long serialVersionUID = 1L;
    private long itemId;
    private long windowEnd;
    private long count;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
