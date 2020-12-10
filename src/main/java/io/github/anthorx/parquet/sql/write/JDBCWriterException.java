package io.github.anthorx.parquet.sql.write;

public class JDBCWriterException extends RuntimeException {

  public JDBCWriterException(String message) {
    super(message);
  }

  public JDBCWriterException(String message, Throwable cause) {
    super(message, cause);
  }
}
