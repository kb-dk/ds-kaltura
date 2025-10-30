package dk.kb.kaltura.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.math.NumberUtils;

public class TopContentDto {
    private String object_id;
    private String entry_name;
    private Integer count_plays;
    private Float sum_time_viewed;
    private Float avg_time_viewed;
    private Integer count_loads;
    private Float load_play_ratio;
    private Float avg_view_drop_off;
    private Integer unique_known_users;

    // Default constructor
    public TopContentDto() {
    }

    // Parameterized constructor
    @JsonCreator
    public TopContentDto(
            @JsonProperty("object_id") String object_id,
            @JsonProperty("entry_name") String entry_name,
            @JsonProperty("count_plays") Integer count_plays,
            @JsonProperty("sum_time_viewed") Float sum_time_viewed,
            @JsonProperty("avg_time_viewed") Float avg_time_viewed,
            @JsonProperty("count_loads") Integer count_loads,
            @JsonProperty("load_play_ratio") Float load_play_ratio,
            @JsonProperty("avg_view_drop_off") Float avg_view_drop_off,
            @JsonProperty("unique_known_users") Integer unique_known_users
    ) {
        this.object_id = object_id;
        this.entry_name = entry_name;
        this.count_plays = count_plays;
        this.sum_time_viewed = sum_time_viewed;
        this.avg_time_viewed = avg_time_viewed;
        this.count_loads = count_loads;
        this.load_play_ratio = load_play_ratio;
        this.avg_view_drop_off = avg_view_drop_off;
        this.unique_known_users = unique_known_users;
    }

    public String getObject_id() {
        return object_id;
    }

    public void setObject_id(String object_id) {
        this.object_id = object_id;
    }

    public String getEntry_name() {
        return entry_name;
    }

    public void setEntry_name(String entry_name) {
        this.entry_name = entry_name;
    }

    public Integer getCount_plays() {
        return count_plays;
    }

    public void setCount_plays(Integer count_plays) {
        this.count_plays = count_plays;
    }

    public Float getSum_time_viewed() {
        return sum_time_viewed;
    }

    public void setSum_time_viewed(Float sum_time_viewed) {
        this.sum_time_viewed = sum_time_viewed;
    }

    public Float getAvg_time_viewed() {
        return avg_time_viewed;
    }

    public void setAvg_time_viewed(Float avg_time_viewed) {
        this.avg_time_viewed = avg_time_viewed;
    }

    public Integer getCount_loads() {
        return count_loads;
    }

    public void setCount_loads(Integer count_loads) {
        this.count_loads = count_loads;
    }

    public Float getLoad_play_ratio() {
        return load_play_ratio;
    }

    public void setLoad_play_ratio(Float load_play_ratio) {
        this.load_play_ratio = load_play_ratio;
    }

    public Float getAvg_view_drop_off() {
        return avg_view_drop_off;
    }

    public void setAvg_view_drop_off(Float avg_view_drop_off) {
        this.avg_view_drop_off = avg_view_drop_off;
    }

    public Integer getUnique_known_users() {
        return unique_known_users;
    }

    public void setUnique_known_users(Integer unique_known_users) {
        this.unique_known_users = unique_known_users;
    }

    @Override
    public String toString() {
        return "reportDto{" +
                "object_id='" + object_id + '\'' +
                ", entry_name='" + entry_name + '\'' +
                ", count_plays=" + count_plays +
                ", sum_time_viewed=" + sum_time_viewed  +
                ", avg_time_viewed=" + avg_time_viewed +
                ", count_loads=" + count_loads +
                ", load_play_ratio=" + load_play_ratio +
                ", avg_view_drop_off=" + avg_view_drop_off +
                ", unique_known_users=" + unique_known_users +
                '}';
    }
}