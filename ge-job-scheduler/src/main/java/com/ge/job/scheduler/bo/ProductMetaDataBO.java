package com.ge.job.scheduler.bo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by Srinath on 11/28/2016.
 */
@Getter
@Setter
public class ProductMetaDataBO {

    @JsonProperty("metadata")
    private CrunchBaseMetadataBO metadata;
    @JsonProperty("data")
    private ProductCrunchBaseDataBO data;

}
