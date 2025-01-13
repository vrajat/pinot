package org.apache.pinot.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.AddLogicalTableCommand;


public class LogicalTableQuickStart extends MultistageEngineQuickStart {
  private static final String QUICKSTART_IDENTIFIER = "LOGICAL";
  private static final String[] TPCH_DIRECTORIES = new String[]{
      "examples/batch/virtual/nation1",
      "examples/batch/virtual/nation2",
  };

  public void createLogicalTable(File logicalTableConfigFile) throws Exception {
    AddLogicalTableCommand addLogicalTableCommand = new AddLogicalTableCommand();
    addLogicalTableCommand.setLogicalTableConfigFile(logicalTableConfigFile.getAbsolutePath())
        .setControllerHost("localhost")
        .setControllerPort(String.valueOf(9000))
        .setExecute(true);
    if (!addLogicalTableCommand.execute()) {
      throw new RuntimeException("Failed to create logical table with config file - " + logicalTableConfigFile);
    }
  }

  @Override
  public void execute()
      throws Exception {
    super.execute();
    createLogicalTable(new File("examples/batch/virtual/nation_logical_table.json"));
  }

  @Override
  public List<String> types() {
    return Collections.singletonList(QUICKSTART_IDENTIFIER);
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return TPCH_DIRECTORIES;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 1;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", QUICKSTART_IDENTIFIER));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
