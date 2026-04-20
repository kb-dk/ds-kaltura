package dk.kb.kaltura.domain;

public class ReportTableDto {
    private String header;
    private String data;
    private int totalCount;

    // Default constructor
    public ReportTableDto() {
    }

    // Parameterized constructor
    public ReportTableDto(String header, String data, int totalCount) {
        this.header = header;
        this.data = data;
        this.totalCount = totalCount;
    }

    // Getters and Setters
    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    public String toString() {
        return "ReportTableDTO{" +
                "header='" + header + '\'' +
                ", data='" + data + '\'' +
                ", totalCount=" + totalCount +
                '}';
    }
}