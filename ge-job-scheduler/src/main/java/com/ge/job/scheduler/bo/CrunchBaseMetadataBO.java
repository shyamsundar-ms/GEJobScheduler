package com.ge.job.scheduler.bo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by Srinath on 11/28/2016.
 */
@Getter
@Setter
public class CrunchBaseMetadataBO {

    private int version;

    @JsonProperty("www_path_prefix")
    private String wwwPathPrefix;

    @JsonProperty("api_path_prefix")
    private String apiPathPrefix;

    @JsonProperty("image_path_prefix")
    private String imagePathPrefix;

}
