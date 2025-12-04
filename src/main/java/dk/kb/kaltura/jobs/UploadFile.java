package dk.kb.kaltura.jobs;

import dk.kb.kaltura.BuildInfoManager;
import dk.kb.kaltura.client.DsKalturaClient;

import picocli.CommandLine;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.enums.MediaType;

/**
 * <p>
 * The script uploadfile.sh will call this class main method. If the video/audio has been uploaded sucessfully, the internal KalturaID will be logged  
 * The script takes 1 arguments and 5 options that has meta-data information
 * </p>
 * <ul>
 *   <li>argument 1) filePath - The absolute path to the video or audio file</li>
 *   <li>option -referenceId or --referenceId. The referenceId we have for the record so it can later be found in Kaltura </li>
 *   <li>option -type or --type. Example: VIDEO or AUDIO</li>
 *   <li>option -title or --title.  The title of the video/audio. This can be  configured to be shown in the Kaltura player </li>
 *   <li>option -description or --description. A longer description of the video/audio. This can configured to be shown in the Kaltura player</li>
 *   <li>option -tag  or --tag. Use 'DR-KULTURA' since all uploaded record that then be found easy in Kaltura/li>
 * </ul>
 */
public class UploadFile extends JobsBase implements Callable<Integer>{ 

    private static final Logger log = LoggerFactory.getLogger(UploadFile.class);

    public enum MEDIATYPES {VIDEO, AUDIO}  //There are more types in Kaltura, but only support these for now.


    @CommandLine.Parameters(index = "0", type = String.class) //Required
    private String filePath;
    
    
    @CommandLine.Option(names = {"-referenceid", "--referenceid"}, required = true, type = String.class, 
                                   description = "The referenceId given to the entry at Kaltura.")
    private String referenceId;

    @CommandLine.Option(names = {"-fileExtension", "--fileExtension"}, required = true, type = String.class,
            description = "The referenceId given to the entry at Kaltura.")
    private String fileExtension;

    @CommandLine.Option(names = {"-type", "--type"}, required = true, type =  MEDIATYPES.class,
            description = "Valid values: ${COMPLETION-CANDIDATES}")
    private MEDIATYPES mediatype;    

    @CommandLine.Option(names = {"-title", "--title"}, required = true, type = String.class, 
            description = "The title(name) for the entry in Kaltura")   
    private String title;

    @CommandLine.Option(names = {"-description", "--description"}, required = true, type = String.class, 
            description = "The description for the entry in Kaltura")    
    private String description;

    @CommandLine.Option(names = {"-tag", "--tag"}, required = true, type = String.class, 
            description = "The tag given to the entry. Tag works a collection identifier at Kaltura. Recommended value is 'DS-KALTURA'")
    private String tag;

    @CommandLine.Option(names = {"-flavorId", "--flavorId"}, required = true, type = String.class,
            description = "Id of the flavor the media will be uploaded as.")
    private Integer flavorId;

    /*
     * Implement the normal 'main' method here
     */
    @Override
    public Integer call() throws Exception {        

        MediaType kalturaMediaType=null;
        switch (mediatype) {
        case VIDEO:
            kalturaMediaType=MediaType.VIDEO;
            break;              

        case AUDIO:
            kalturaMediaType=MediaType.AUDIO;
            break;              
        } 

        DsKalturaClient kalturaClient = getKalturaClient();
        String kalturaId=kalturaClient.uploadMedia(filePath, referenceId, kalturaMediaType, title,
                description, tag, flavorId, fileExtension);
        String message="Upload succes. Entry has kalturaId:"+kalturaId;
        log.info(message);
        System.out.println(message);
        return 0; //Exit code
    }


    public static void main(String... args) {
        BuildInfoManager.logApplicationInfo(); // Mandated by Operations
        System.out.println("Arguments passed by commandline is: " + Arrays.asList(args));

        CommandLine app = new CommandLine(new UploadFile());
        int exitCode = app.execute(args);
        SystemControl.exit(exitCode);
    }



    /**
     * Handles communication with the calling environment. Currently only in the form of sending the proper exit code.
     */
    static class SystemControl {
        static void exit(int exitCode) {
            if (exitCode == 0) {
                log.info("Exiting with code 0 (success)");
            } else {
                log.error("Exiting with code " + exitCode + " (fail)");
            }
            System.exit(exitCode);
        }
    }

}