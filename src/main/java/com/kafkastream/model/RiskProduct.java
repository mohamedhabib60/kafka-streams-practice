package com.kafkastream.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RiskProduct {
    private String id;
    private String name;
    private int stock;
    boolean isTrendProduct;
}
