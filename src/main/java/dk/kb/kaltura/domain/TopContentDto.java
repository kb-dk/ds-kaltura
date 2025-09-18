package dk.kb.kaltura.domain;

import org.apache.commons.lang3.math.NumberUtils;

public class TopContentDto {
    public static final String HEADER = "object_id,entry_name,count_plays,sum_time_viewed,avg_time_viewed," +
            "count_loads,load_play_ratio,avg_view_drop_off,unique_known_users";
    public static final int HEADER_SIZE = HEADER.split(",").length;
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
    public TopContentDto(String object_id, String entry_name, String count_plays, String sum_time_viewed,
                         String avg_time_viewed, String count_loads, String load_play_ratio,
                         String avg_view_drop_off, String unique_known_users) {
        this.object_id = object_id;
        this.entry_name = entry_name;
        this.count_plays = Integer.getInteger(count_plays);
        this.sum_time_viewed = NumberUtils.createFloat(sum_time_viewed);
        this.avg_time_viewed = NumberUtils.createFloat(avg_time_viewed);
        this.count_loads = NumberUtils.createInteger(count_loads);
        this.load_play_ratio = NumberUtils.createFloat(load_play_ratio);
        this.avg_view_drop_off = NumberUtils.createFloat(avg_view_drop_off);
        this.unique_known_users = NumberUtils.createInteger(unique_known_users);
    }

    public TopContentDto(String header, String... args) {
        if(!header.equals(HEADER)){
            throw new IllegalArgumentException("Report map contains invalid header '" + header  + "'  does" +
                " not match '" + TopContentDto.HEADER + "'");
        }

        if (args.length != HEADER_SIZE) {
            throw new IllegalArgumentException("Report map contains invalid data '" + args + "'");
        }

        this.object_id = args[0];
        this.entry_name = args[1];
        this.count_plays = NumberUtils.createInteger(args[2]);
        this.sum_time_viewed = NumberUtils.createFloat(args[3]);
        this.avg_time_viewed = NumberUtils.createFloat(args[4]);
        this.count_loads = NumberUtils.createInteger(args[5]);
        this.load_play_ratio = NumberUtils.createFloat(args[6]);
        this.avg_view_drop_off = NumberUtils.createFloat(args[7]);
        this.unique_known_users = Integer.parseInt(args[8]);
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