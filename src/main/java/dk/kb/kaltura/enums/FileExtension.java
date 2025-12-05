package dk.kb.kaltura.enums;

import java.util.Arrays;
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

    public static boolean checkExtension(String filePath, FileExtension expectedExtension) {
                boolean filePathMatchedUnexpected = Arrays.stream(FileExtension.values())
                        .filter(fileExtension -> !fileExtension.equals(expectedExtension))
                        .anyMatch(fileExtension -> filePath.toLowerCase(Locale.ROOT).endsWith(fileExtension.getExtension()));
                if(filePathMatchedUnexpected) {
                    throw new IllegalArgumentException("file path: '" + filePath + "' has valid extension not matching expected" +
                            " extension: " + expectedExtension.getExtension());
                }
                return filePath.toLowerCase(Locale.ROOT).endsWith(expectedExtension.getExtension());
    }
}
