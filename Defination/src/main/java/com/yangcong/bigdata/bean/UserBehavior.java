package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-02-04 17:10
 */
public class UserBehavior implements Serializable {
    private static final long serialVersionUID=1L;
    private Long userId;
    private Long itermId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItermId() {
        return itermId;
    }

    public void setItermId(Long itermId) {
        this.itermId = itermId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
