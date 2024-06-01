package tukano.impl.java.servers;

import org.pac4j.scribe.builder.api.DropboxApi20;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import tukano.api.java.Result;
import static java.lang.String.format;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.CONFLICT;
import static tukano.api.java.Result.ErrorCode.FORBIDDEN;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.NOT_FOUND;

import java.io.File;
import java.util.logging.Logger;
import tukano.api.java.Result.ErrorCode;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.java.clients.Clients;
import tukano.impl.java.servers.msgs.CreateFolderV2Args;
import tukano.impl.java.servers.msgs.DropboxDeleteArgs;
import tukano.impl.java.servers.msgs.DropboxDownloadArg;
import tukano.impl.java.servers.msgs.DropboxUploadArg;
import utils.Token;

public class JavaBlobsProxy implements ExtendedBlobs {

    private static final String apiKey = "z8ew8syaoomplrd";
    private static final String apiSecret = "mzhz3lfnmx16xaf";
    private static final String accessTokenStr = "sl.B2XIuzCKWBSBRTOBAAjiLTAdF79RR_G9byQAj9MAj5r-FYuucY9q7yJuXLgUts1pqMWpwbxoMyhLFuuU8pX3DEe4magUwVJIzcf_rVX1tKtgacWjGGFa2SACiai18E1AtImYukt9jOlV";
    private static final String UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
    private static final String DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String DELETE_URL = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
    private static final String CONTENT_TYPE_HDR = "Content-Type";
    private static final String DROPBOXAPIARG = "Dropbox-API-Arg";
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final int HTTP_SUCCESS = 200;
    private static final Logger Log = Logger.getLogger(JavaBlobsProxy.class.getName());

    private final Gson json;
    private final OAuth2AccessToken accessToken;
    private final OAuth20Service service;
    private static final String BLOBS_ROOT_DIR = "/tmp/blobs/";

    public JavaBlobsProxy() {
        json = new Gson();
        accessToken = new OAuth2AccessToken(accessTokenStr);
        service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    }

    @Override
    public Result<Void> upload(String blobId, byte[] bytes) {

        if (!validBlobId(blobId))
            return error(FORBIDDEN);

        var file = toFilePath(blobId);
        if (file == null)
            return error(BAD_REQUEST);

        try {
            var filepath = file.getAbsolutePath(); // todo

            var upload = new OAuthRequest(Verb.POST, UPLOAD_URL);
            upload.addHeader(DROPBOXAPIARG,
                    json.toJson(new DropboxUploadArg(false, "overwrite", false, filepath, false)));
            upload.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);

            upload.setPayload(bytes);

            service.signRequest(accessToken, upload);

            // Execute the request
            Response response = service.execute(upload);

            // Check if the response is successful
            if (response.getCode() != HTTP_SUCCESS) {
                Log.info(response.getBody());
                return Result.error(ErrorCode.INTERNAL_ERROR);
            }

            // Return success result
            return Result.ok();

        } catch (Exception e) {
            Log.severe(() -> format("Error uploading blobId = %s from Dropbox: %s", blobId, e.getMessage()));
            throw new RuntimeException(e);
        }

    }

    public Result<byte[]> download(String blobId) {
        Log.info(() -> format("download : blobId = %s\n", blobId));

        var file = toFilePath(blobId);
        if (file == null)
            return error(BAD_REQUEST);

        try {
            String filepath = file.getAbsolutePath();
            String dropboxApiArg = json.toJson(new DropboxDownloadArg(filepath));

            var download = new OAuthRequest(Verb.POST, DOWNLOAD_URL);
            download.addHeader(DROPBOXAPIARG, dropboxApiArg);
            download.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);

            service.signRequest(accessToken, download);

            Response response = service.execute(download);

            if (response.getCode() != HTTP_SUCCESS) {
                Log.info(response.getBody());
                return Result.error(ErrorCode.INTERNAL_ERROR);
            }

            return Result.ok(response.getStream().readAllBytes());

        } catch (Exception e) {
            Log.severe(() -> format("Error downloading blobId = %s from Dropbox: %s", blobId, e.getMessage()));
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> delete(String blobId, String token) {

        if (!Token.matches(token))
            return error(FORBIDDEN);

        var file = toFilePath(blobId);

        if (file == null)
            return error(BAD_REQUEST);

        try {
            String filepath = file.getAbsolutePath();
            String jsonArgs = json.toJson(new DropboxDeleteArgs(filepath));
            var delete = new OAuthRequest(Verb.POST, DELETE_URL);
            delete.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            delete.setPayload(jsonArgs);

            service.signRequest(accessToken, delete);

            Response r = service.execute(delete);

            if (r.getCode() != HTTP_SUCCESS) {
                Log.info(r.getBody());
                return Result.error(ErrorCode.INTERNAL_ERROR);
            }
            return Result.ok();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<Void> deleteAllBlobs(String userId, String token) {

        if (!Token.matches(token)) {
            return error(FORBIDDEN);
        }

        // Construct the directory path for the user's blobs
        String userDirectory = BLOBS_ROOT_DIR + userId;

        try {
            // Create the delete request to remove the user's directory
            var deleteRequest = new OAuthRequest(Verb.POST, DELETE_URL);
            deleteRequest.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            deleteRequest.setPayload(json.toJson(new DropboxDeleteArgs(userDirectory)));

            service.signRequest(accessToken, deleteRequest);

            Response deleteResponse = service.execute(deleteRequest);
            if (deleteResponse.getCode() != HTTP_SUCCESS) {
                Log.info(deleteResponse.getBody());
                return Result.error(ErrorCode.INTERNAL_ERROR);
            }

            return Result.ok();

        } catch (Exception e) {
            Log.severe(() -> format("Error deleting all blobs for userId = %s: %s", userId, e.getMessage()));
            return error(INTERNAL_ERROR);
        }
    }

    private boolean validBlobId(String blobId) {
        return Clients.ShortsClients.get().getShort(blobId).isOK();
    }

    private File toFilePath(String blobId) {
        var parts = blobId.split("-");
        if (parts.length != 2) {
            return null;
        }

        String directoryName = BLOBS_ROOT_DIR + parts[0];
        var res = new File(directoryName + "/" + parts[1]);

        // Ensure the directory exists or create it using OAuth
        var createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
        createFolder.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        createFolder.setPayload(json.toJson(new CreateFolderV2Args(directoryName, false)));

        service.signRequest(accessToken, createFolder);

        try {
            Response r = service.execute(createFolder);
            if (r.getCode() != HTTP_SUCCESS && r.getCode() != 409) {
                throw new RuntimeException(String.format(
                        "Failed to create directory: %s, Status: %d, \nReason: %s\n",
                        directoryName, r.getCode(), r.getBody()));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute OAuth request", e);
        }

        return res;
    }

}
