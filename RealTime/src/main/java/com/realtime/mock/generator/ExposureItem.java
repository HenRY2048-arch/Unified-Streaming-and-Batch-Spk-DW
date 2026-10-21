package com.realtime.mock.generator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** 曝光批次里的一条候选（`exposure_batch.items[]`）。 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExposureItem {
    @JsonProperty("item_id")
    private Long itemId;

    @JsonProperty("position")
    private Integer position;

    @JsonProperty("rank_score")
    private Double rankScore;

    public Long getItemId() { return itemId; }
    public void setItemId(Long itemId) { this.itemId = itemId; }

    public Integer getPosition() { return position; }
    public void setPosition(Integer position) { this.position = position; }

    public Double getRankScore() { return rankScore; }
    public void setRankScore(Double rankScore) { this.rankScore = rankScore; }
}
