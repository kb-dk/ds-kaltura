package dk.kb.kaltura.enums;

public enum MediaFileExtension {
    MP3(".mp3"),
    MP4(".mp4");

    private final String extension;

    MediaFileExtension(String extension) {
        if (extension == null || extension.isEmpty()){
            throw new IllegalArgumentException("MediaFileExtension must not be null or empty");
        }

        if (!extension.startsWith(".")){
            extension = "." + extension;
        }
        extension = extension.toLowerCase();

        this.extension = extension;
    }

}
