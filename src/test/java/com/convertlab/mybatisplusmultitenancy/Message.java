package com.convertlab.mybatisplusmultitenancy;

import lombok.Data;

@Data
public class Message {
    private Long id;
    private Long userId;
    private Long tenantId;
    private String msg;
}
