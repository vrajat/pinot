package org.apache.pinot.spi.exception;

/**
 * The {@code EarlyTerminationException} can be thrown from {Operator#nextBlock()} when the operator detects that
 * the query has been terminated by the system rather than by the client or due to an error.
 */
public class SystemTerminatedException extends RuntimeException {
  public SystemTerminatedException(String message) {
    super(message);
  }
}
