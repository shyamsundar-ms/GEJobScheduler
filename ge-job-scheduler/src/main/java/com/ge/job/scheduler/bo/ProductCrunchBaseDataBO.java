package com.ge.job.scheduler.bo;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Created by Srinath on 11/28/2016.
 */
@Getter
@Setter
public class ProductCrunchBaseDataBO {
    private ProductBO paging;
    private List<ProductItemsBO> items;

}
