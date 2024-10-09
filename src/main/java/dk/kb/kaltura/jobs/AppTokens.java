package dk.kb.kaltura.jobs;

import com.kaltura.client.types.AppToken;
import dk.kb.kaltura.BuildInfoManager;
import dk.kb.kaltura.client.AppTokenClient;
import dk.kb.kaltura.config.ServiceConfig;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class AppTokens implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "operation [add,delete,list]")
    private String operation;

    @CommandLine.Option(names = {"-i", "--tokenId"}, description = "Token ID")
    private String tokenId;

    @CommandLine.Option(names = {"-s", "--adminsecret"}, description = "Admin secret", required = true)
    private String adminSecret;

    @CommandLine.Option(names = {"-d", "--description"}, description = "App token description (add token)")
    private String description = "Basic app token";

    @Override
    public Integer call() throws Exception {
        ServiceConfig.initialize(System.getProperty("dk.kb.applicationConfig"));
        AppTokenClient client = new AppTokenClient(adminSecret);
        switch (operation) {
            case "add":
                System.out.println("Adding app token");
                AppToken appToken = client.addAppToken(description);
                prettyPrintAppToken(appToken);
                break;
            case "delete":
                System.out.println("Deleting app token");
                client.deleteAppToken(tokenId);
                break;
            case "list":
                System.out.println("Listing app tokens");
                client.listAppTokens().stream().forEach(this::prettyPrintAppToken);
                break;
            default:
                System.err.println("Unknown operation: " + operation);
                return 0;
        }
        return 0;
    }

    public static void main(String... args) {
        BuildInfoManager.logApplicationInfo(); // Mandated by Operations
        System.out.println("Arguments passed by commandline is: " + Arrays.asList(args));

        CommandLine app = new CommandLine(new AppTokens());
        int exitCode = app.execute(args);
        IdLookup.SystemControl.exit(exitCode);

    }

    private void prettyPrintAppToken(AppToken appToken) {
        System.out.println("AppToken (id: " + appToken.getId() +
                ", token: " + appToken.getToken() +
                ", createdAt: " + appToken.getCreatedAt() +
                ", expires: " + appToken.getExpiry() +
                ", description: " + appToken.getDescription() + ")");
    }
}
