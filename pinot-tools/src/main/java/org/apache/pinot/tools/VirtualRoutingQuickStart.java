package org.apache.pinot.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tools.admin.PinotAdministrator;


public class VirtualRoutingQuickStart extends MultistageEngineQuickStart {
  private static final String QUICKSTART_IDENTIFIER = "VIRTUAL";
  private static final String[] TPCH_DIRECTORIES = new String[]{
      "examples/batch/virtual/nation1",
      "examples/batch/virtual/nation2",
  };

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
