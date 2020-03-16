package com.yangcong.bigdata.bean;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-02-07 16:13
 */
public class OrderResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long orderId;
    private String resultMsg;

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }
}
