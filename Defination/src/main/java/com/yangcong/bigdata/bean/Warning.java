package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-01-19 15:09
 */
public class Warning implements Serializable {
    private static final long serialVersionUID=1L;
    private long userId;
    private long firstFailTime;
    private long lastFailTime;
    private String warningMsg;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "Warning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
