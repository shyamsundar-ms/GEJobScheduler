package com.ge.job.scheduler.bo;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by Srinath on 11/28/2016.
 */

@Getter
@Setter
public class ProductItemsBO {

    private String type;
    private String uuid;
    private ProductPropertiesBO properties;

    @Override
    public String toString() {
        return "ProductItemsBO{" +
                "type='" + type + '\'' +
                ", uuid='" + uuid + '\'' +
                ", properties=" + properties +
                '}';
    }
}
