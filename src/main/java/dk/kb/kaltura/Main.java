package dk.kb.kaltura;

import java.util.Arrays;
import java.util.concurrent.Callable;


import dk.kb.kaltura.config.ServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

//Check link below for examples.
//https://picocli.info/#_introduction
@CommandLine.Command()
public class Main implements Callable<Integer>{
        
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public enum LangEnum {DA, EN} // Language enums in example
    
    /*
     *  Example with default value
     */    
    @CommandLine.Parameters(index = "0", type = String.class, defaultValue = "John Doe")
    private String name;
    
    /*
     * Example with class type required (integer)
     */
    @CommandLine.Parameters(index = "1", type = Integer.class)
    private int age;
    

    /*
     * Example to see specify a format with enum types
     * Both -lang=DK and --lang=DK will be accepted
     */
    @CommandLine.Option(names = {"-lang", "--lang"}, required = false, type = LangEnum.class,
                        description = "Valid values: ${COMPLETION-CANDIDATES}", defaultValue = "EN")
    private LangEnum lang;
    
    /*
     * Implement the normal 'main' method here
     */
    @Override
    public Integer call() throws Exception {        
   
     // How to load a property
     //When debugging from IDEA, add -Ddk.kb.applicationConfig="src/main/conf/kaltura-*.yaml" to "VM options"
        ServiceConfig.initialize(System.getProperty("dk.kb.applicationConfig"));

        String speaker = ServiceConfig.getSpeaker();
        
        HelloWorld hw = new HelloWorld();        
        String message= hw.sayHello(name, age, speaker, lang);
        System.out.println(message);                        
        return 0; //Exit code
    }
    
    
    public static void main(String... args) {
        BuildInfoManager.logApplicationInfo(); // Mandated by Operations
        System.out.println("Arguments passed by commandline is: " + Arrays.asList(args));
        CommandLine app = new CommandLine(new Main());
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