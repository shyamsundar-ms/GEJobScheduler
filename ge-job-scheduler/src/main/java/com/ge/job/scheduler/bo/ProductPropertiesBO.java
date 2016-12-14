package com.ge.job.scheduler.bo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by Srinath on 11/28/2016.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductPropertiesBO {

    private String permalink;
    @JsonProperty("api_path")
    private String apiPath;
    @JsonProperty("web_path")
    private String webPath;
    private String name;
    @JsonProperty("short_description")
    private String shortDescription;
    @JsonProperty("owner_permalink")
    private String ownerPermalink;
    @JsonProperty("owner_api_path")
    private String ownerApiPath;
    @JsonProperty("owner_web_path")
    private String ownerWebPath;
    @JsonProperty("owner_name")
    private String ownerName;
    @JsonProperty("profile_image_url")
    private String profileImageUrl;
    @JsonProperty("homepage_url")
    private String homepageUrl;
    @JsonProperty("facebook_url")
    private String facebookUrl;
    @JsonProperty("twitter_url")
    private String twitterUrl;
    @JsonProperty("linkedin_url")
    private String linkedinUrl;
    @JsonProperty("created_at")
    private int createdAt;
    @JsonProperty("updated_at")
    private int updatedAt;
    @JsonProperty("blog_url")
    private String blogUrl;
    @JsonProperty("google+_url")
    private String googleUrl;
    @JsonProperty("instagram_url")
    private String instagramUrl;
    @JsonProperty("pinterest_url")
    private String pinterestUrl;

    @Override
    public String toString() {
        return "ProductPropertiesBO{" +
                "permalink='" + permalink + '\'' +
                ", apiPath='" + apiPath + '\'' +
                ", webPath='" + webPath + '\'' +
                ", name='" + name + '\'' +
                ", shortDescription='" + shortDescription + '\'' +
                ", ownerPermalink='" + ownerPermalink + '\'' +
                ", ownerApiPath='" + ownerApiPath + '\'' +
                ", ownerWebPath='" + ownerWebPath + '\'' +
                ", ownerName='" + ownerName + '\'' +
                ", profileImageUrl='" + profileImageUrl + '\'' +
                ", homepageUrl='" + homepageUrl + '\'' +
                ", facebookUrl='" + facebookUrl + '\'' +
                ", twitterUrl='" + twitterUrl + '\'' +
                ", linkedinUrl='" + linkedinUrl + '\'' +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", blogUrl='" + blogUrl + '\'' +
                ", googleUrl='" + googleUrl + '\'' +
                ", instagramUrl='" + instagramUrl + '\'' +
                ", pinterestUrl='" + pinterestUrl + '\'' +
                '}';
    }
}
