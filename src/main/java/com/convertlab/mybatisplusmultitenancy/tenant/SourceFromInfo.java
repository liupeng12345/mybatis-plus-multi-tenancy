package com.convertlab.mybatisplusmultitenancy.tenant;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SourceFromInfo {
    private String alias;
    private String tableName;
    private Boolean needAddCondition;
}
