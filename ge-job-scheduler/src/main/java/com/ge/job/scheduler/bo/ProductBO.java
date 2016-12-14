package com.ge.job.scheduler.bo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
/**
 * Created by Srinath on 11/28/2016.
 */

@Getter
@Setter
public class ProductBO {


    @JsonProperty("total_items")
    private int totalItems;

    @JsonProperty("number_of_pages")
    private int numberOfPages;

    @JsonProperty("current_page")
    private int currentPage;

    @JsonProperty("sort_order")
    private String sortOrder;

    @JsonProperty("items_per_page")
    private int itemsPerPage;

    @JsonProperty("next_page_url")
    private String nextPageUrl;

    @JsonProperty("prev_page_url")
    private String prevPageUrl;

    @Override
    public String toString() {
        return "ProductBO{" +
                "totalItems=" + totalItems +
                ", numberOfPages=" + numberOfPages +
                ", currentPage=" + currentPage +
                ", sortOrder='" + sortOrder + '\'' +
                ", itemsPerPage=" + itemsPerPage +
                ", nextPageUrl='" + nextPageUrl + '\'' +
                ", prevPageUrl='" + prevPageUrl + '\'' +
                '}';
    }
}
