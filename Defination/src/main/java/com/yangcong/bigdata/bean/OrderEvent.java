package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-02-07 16:07
 */
public class OrderEvent implements Serializable {
    private static final long serialVersionUID=1L;
    private Long orderId;
    private String eventType;
    private String txId;
    private long timestamp;

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
