package dk.kb.kaltura.jobs;



import dk.kb.kaltura.BuildInfoManager;
import dk.kb.kaltura.client.DsKalturaClient;

import picocli.CommandLine;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.enums.MediaType;

public class UploadFile extends JobsBase implements Callable<Integer>{ 

    private static final Logger log = LoggerFactory.getLogger(UploadFile.class);

    public enum MEDIATYPES {VIDEO, AUDIO}
    
    
    
    @CommandLine.Parameters(index = "0", type = String.class) //Required
    private String filePath;
        
    @CommandLine.Parameters(index = "1", type = String.class) //Required
    private String referenceId;
    
    @CommandLine.Parameters(index = "2", type = MEDIATYPES.class, description = "Valid values: ${COMPLETION-CANDIDATES}", defaultValue = "VIDEO")
    private MEDIATYPES mediatype;
    
    @CommandLine.Parameters(index = "3", type = String.class) //Required
    private String title;
    
    @CommandLine.Parameters(index = "4", type = String.class) //Required
    private String description;
    
    @CommandLine.Parameters(index = "5", type = String.class) //Required
    private String tag;
    
    
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
       String kalturaId=kalturaClient.uploadMedia(filePath,referenceId,kalturaMediaType,title,description,tag);
       String message="ReferenceId:"+referenceId +" -> kalturaId:"+kalturaId;
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