package com.yangcong.bigdata.bean;

/**
 * Created by mouzwang on 2020-01-19 15:09
 */
public class Warning {
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
}
