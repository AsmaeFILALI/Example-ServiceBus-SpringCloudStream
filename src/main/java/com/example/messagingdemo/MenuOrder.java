package com.example.messagingdemo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MenuOrder implements Serializable {
    @JsonProperty
    private String orderIdentifier;
    @JsonProperty
    private int orderId;
    @JsonProperty
    private List<String> orderList;
    @JsonProperty
    private String customerName;

    @Override
    public String toString() {
        return "MenuOrder{" +
                "orderIdentifier='" + orderIdentifier + '\'' +
                ", orderId=" + orderId +
                ", orderList=" + orderList +
                ", customerName='" + customerName + '\'' +
                '}';
    }
}