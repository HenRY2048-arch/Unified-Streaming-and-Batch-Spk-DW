package com.realtime.mock.generator;

import java.util.List;

public class Item {
    private Long itemId;
    private Long authorId;
    private String category;
    private List<String> tags;
    private Integer contentLen;
    private String publishedTs;

    public Item(Long itemId, Long authorId, String category, List<String> tags,
                Integer contentLen, String publishedTs) {
        this.itemId = itemId;
        this.authorId = authorId;
        this.category = category;
        this.tags = tags;
        this.contentLen = contentLen;
        this.publishedTs = publishedTs;
    }

    public Long getItemId() { return itemId; }
    public Long getAuthorId() { return authorId; }
    public String getCategory() { return category; }
    public List<String> getTags() { return tags; }
    public Integer getContentLen() { return contentLen; }
    public String getPublishedTs() { return publishedTs; }
}
