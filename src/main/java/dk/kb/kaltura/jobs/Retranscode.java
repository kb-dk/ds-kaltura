package dk.kb.kaltura.jobs;

import com.kaltura.client.types.APIException;
import com.kaltura.client.types.MediaEntryFilter;
import dk.kb.kaltura.BuildInfoManager;
import dk.kb.kaltura.client.DsKalturaClient;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.util.yaml.YAML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class Retranscode extends JobsBase implements Callable<Integer> {
    private static final Logger log = LoggerFactory.getLogger(Retranscode.class);


    final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");

    @CommandLine.Option(names = {"-IdIn", "--IdIn"}, required = false, type =
            String.class,
            description = "Filter by id")
    private String IdIn;

    @CommandLine.Option(names = {"-TagsIn", "--TagsIn"}, required = false, type =
            String.class,
            description = "filter by tags seperated by comma")
    private String TagsIn;

    @CommandLine.Option(names = {"-OutfileDir", "--OutfileDir"}, required = false, type =
            String.class,
            description = "filter by tags seperated by comma")
    private String OutfileDir;

    @Override
    public Integer call() throws Exception {
        DsKalturaClient clientSession = getClient();
        int conversionProfileIdAudio = conf.getInteger("conversionProfileIdAudio");
        int conversionProfileIdVideo = conf.getInteger("conversionProfileIdVideo");
        int videoFlavorParamId = 3;
        int audioFlavorParamId = 359;

        MediaEntryFilter filter = new MediaEntryFilter();

        String failTag = DsKalturaClient.DEFAULT_TRANSCODE_ERROR_TAG;
        String successTag = DsKalturaClient.DEFAULT_TRANSCODE_TAG;
        if (TagsIn != null) {
            filter.setTagsLike(TagsIn + "!" + failTag + "!" + successTag);
        } else {
            filter.setTagsLike("*!" + failTag + "!" + successTag);
        }
        filter.setFlavorParamsIdsMatchOr(videoFlavorParamId + "," + audioFlavorParamId);

        if (IdIn != null) {
            filter.setIdIn(IdIn);
        }

        int partnerId = ServiceConfig.getConfig().getSubMap("kaltura").getInteger("partnerId");
        String outputFilename = OutfileDir + "/retranscoded-" + partnerId + "-" +
                LocalDateTime.now(ZoneId.systemDefault()) + ".csv";

        int count = clientSession.countMediaEntry(filter);
        log.info("Entries remaining: {}", count);
        clientSession.updateAllContent(filter, audioFlavorParamId, videoFlavorParamId,
                conversionProfileIdAudio, conversionProfileIdVideo, outputFilename, successTag, failTag);
        return 0;
    }

    public static void main(String... args) throws IOException {
        BuildInfoManager.logApplicationInfo(); // Mandated by Operations
        System.out.println("Arguments passed by commandline is: " + Arrays.asList(args));
        ServiceConfig.initialize("src/main/conf/ds-kaltura-*.yaml");
        CommandLine app = new CommandLine(new Retranscode());
        int exitCode = app.execute(args);
        UploadFile.SystemControl.exit(exitCode);
    }


    private DsKalturaClient getClient() throws APIException {
        return new DsKalturaClient(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("token"),
                conf.getString("tokenId"),
                conf.getString("adminSecret", null),
                conf.getInteger("sessionDurationSeconds"),
                conf.getInteger("sessionRefreshThreshold"),
                conf.getInteger("conversionQueueThreshold"),
                conf.getInteger("conversionQueueDelaySeconds"));
    }


}
