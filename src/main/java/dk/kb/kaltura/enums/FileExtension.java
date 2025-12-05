package dk.kb.kaltura.enums;

import java.util.Locale;

public enum FileExtension {
    MP3(".mp3"),
    MP4(".mp4");

    private final String extension;

    FileExtension(String mediaFileExtension) {
        this.extension = mediaFileExtension;
    }

    public String getExtension() {
        return extension;
    }

    public static FileExtension fromString(String mediaFileExtension) {
        if (mediaFileExtension == null || mediaFileExtension.isEmpty()){
            throw new IllegalArgumentException("MediaFileExtension must not be null or empty");
        }
        if (!mediaFileExtension.startsWith(".")){
            mediaFileExtension = "." + mediaFileExtension;
        }

        switch (mediaFileExtension.toLowerCase(Locale.ROOT)) {
            case ".mp3":
                return MP3;
            case ".mp4":
                return MP4;
            default:
                throw new IllegalArgumentException("MediaFileExtension '" + mediaFileExtension + "' is invalid");
        }

    }
}
