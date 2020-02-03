package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-01-28 18:02
 */
public class MarketingViewCount implements Serializable {
    private static final long serialVersionUID=1L;
    private String windowStart;
    private String channel;

    @Override
    public String toString() {
        return "MarketingViewCount{" +
                "windowStart='" + windowStart + '\'' +
                ", channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", count=" + count +
                '}';
    }

    private String behavior;
    private long count;

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
