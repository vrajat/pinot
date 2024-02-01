package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class RebalancedTenantQuickStart extends RealtimeQuickStart {
  @Override
  public List<String> types() {
    return Collections.singletonList("REBALANCED");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "REBALANCED"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  @Override
  protected Map<String, String> getDefaultStreamTableDirectories() {
    return ImmutableMap.of(
        "upsertPartialMeetupRsvp", "examples/stream/upsertPartialMeetupRsvp"
    );
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapStreamTableDirectories(quickstartTmpDir);
    /* if enableIsolation = true, then broker has the following tags:
      "listFields": {
        "TAG_LIST": [
          "DefaultTenant_BROKER"
        ]
       }

       Server has:
       "TAG_LIST": [
          "DefaultTenant_OFFLINE",
          "DefaultTenant_REALTIME"
        ]
     */
    final QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 2, 2, 1, quickstartRunnerDir, false, null, getConfigOverrides(), null, true);

    startKafka();
    startAllDataStreams(_kafkaStarter, quickstartTmpDir);

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    runner.createServerTenantWith(0, 1, "restream");
    runner.createServerTenantWith(0, 1, "DefaultTenant");
    runner.createBrokerTenantWith(1, "restream");
    runner.createBrokerTenantWith(1, "DefaultTenant");

    printStatus(Quickstart.Color.GREEN, "Tagged servers with 'restream' and 'default' tenants");

    printStatus(Quickstart.Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();

    printStatus(Quickstart.Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Quickstart.Color.GREEN, "Update the tenant of the table");
    QuickstartTableRequest updateRequest = createTableRequest(
        quickstartRunnerDir,
        "upsertPartialMeetupRsvp",
        "examples/stream/restreamUpsertPartialMeetupRsvp"
        );
    runner.updateTable(updateRequest);
    printStatus(Quickstart.Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
