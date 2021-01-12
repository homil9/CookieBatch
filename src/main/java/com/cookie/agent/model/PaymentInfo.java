package com.cookie.agent.model;

import lombok.Data;

public @Data class PaymentInfo {
	private String impUid;
    private String merchantUid;
    private String applyNum;
    private String status;
    private String pgProvider;
    private String pgMethod;
    private String cancelReson;
    private String buyerName;
    private int amount;
    private String paidAt;
    private int totalCancelledAmount;
    private int cancelledAmount;
    private String cancelledAt;
}
