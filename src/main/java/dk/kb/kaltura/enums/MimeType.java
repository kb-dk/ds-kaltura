package dk.kb.kaltura.enums;

public enum MimeType {
    AUDIO_MPEG("audio/mpeg"),
    VIDEO_MP4("video/mp4");

    private final String mimeType;

    MimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getValue() {
        return mimeType;
    }

    public static MimeType fromFileExtension(MediaFileExtension fileExtension) {
        switch(fileExtension) {
            case MP4:
                return VIDEO_MP4; // Assuming MP4 corresponds to VIDEO_MP4
            case MP3:
                return AUDIO_MPEG; // Assuming MP3 corresponds to AUDIO_MPEG
            default:
                throw new IllegalArgumentException("Unsupported file extension: " + fileExtension);
        }
    }
}
