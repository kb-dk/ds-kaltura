package dk.kb.kaltura.jobs;


import dk.kb.kaltura.BuildInfoManager;
import dk.kb.kaltura.client.DsKalturaClient;

import picocli.CommandLine;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The script idlookup.sh will call this class main method. If the record is found in Kaltura the method will log the internal Kaltura ID.  
 * The script takes 1 argument that must be defined:
 * </p>
 * <ul>
 *   <li>1) referenceId- The referenceId given to the record by us when the file was uploaded to Kaltura</li>
 * </ul>
 */
public class IdLookup extends JobsBase implements Callable<Integer>{ 

    private static final Logger log = LoggerFactory.getLogger(IdLookup.class);

    @CommandLine.Parameters(index = "0", type = String.class) //Required
    private String referenceId;
        
    
    /*
     * Implement the normal 'main' method here
     */
    @Override
    public Integer call() throws Exception {        

       DsKalturaClient kalturaClient = getKalturaClient();
       String kalturaId=kalturaClient.getKulturaInternalId(referenceId); 
       String message="ReferenceId:"+referenceId +" -> kalturaId:"+kalturaId;
       log.info(message);
       System.out.println(message);
       return 0; //Exit code
    }


    public static void main(String... args) {
        BuildInfoManager.logApplicationInfo(); // Mandated by Operations
        System.out.println("Arguments passed by commandline is: " + Arrays.asList(args));
        
        CommandLine app = new CommandLine(new IdLookup());
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