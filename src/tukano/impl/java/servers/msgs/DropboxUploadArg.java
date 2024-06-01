package tukano.impl.java.servers.msgs;

public record DropboxUploadArg(
    boolean autorename,
    String mode,
    boolean mute,
    String path,
    boolean strict_conflict
) {
}
