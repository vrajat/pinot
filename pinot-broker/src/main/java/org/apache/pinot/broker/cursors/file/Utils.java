package org.apache.pinot.broker.cursors.file;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;


public class Utils {
  private static final String URI_SEPARATOR = "/";

  private Utils() {

  }

  public static Path getTempPath(Path localTempDir, String... nameParts) {
    StringBuilder filename = new StringBuilder();
    for (String part : nameParts) {
      filename.append(part).append("_");
    }
    filename.append(Thread.currentThread().getId());
    return localTempDir.resolve(filename.toString());
  }

  public static URI combinePath(URI baseUri, String path)
      throws URISyntaxException {
    String newPath =
        baseUri.getPath().endsWith(URI_SEPARATOR) ? baseUri.getPath() + path : baseUri.getPath() + URI_SEPARATOR + path;
    return new URI(baseUri.getScheme(), baseUri.getHost(), newPath, null);
  }
}
