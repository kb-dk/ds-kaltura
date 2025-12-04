package dk.kb.kaltura.enums;

public enum MimeType {
    AUDIO_MPEG("audio/mpeg"),
    VIDEO_MP4("video/mp4");

    private final String mimeType;

    public String getValue() {
        return mimeType;
    }

    MimeType(String mimeType) {
        this.mimeType = mimeType;
    }
}
