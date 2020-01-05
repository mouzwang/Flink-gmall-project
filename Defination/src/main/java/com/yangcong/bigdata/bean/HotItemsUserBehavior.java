package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-01-04 23:30
 */
public class HotItemsUserBehavior implements Serializable {
    private static final long serialVersionUID=1L;
    private long userId;
    private long itemId;
    private int categoryId;
    private String behaivor;
    private long timestamp;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehaivor() {
        return behaivor;
    }

    public void setBehaivor(String behaivor) {
        this.behaivor = behaivor;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
