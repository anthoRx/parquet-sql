package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.jdbc.ReadRecordConsumer;

import java.sql.SQLException;

public interface IJDBCWriter extends AutoCloseable, ReadRecordConsumer {
  void addBatch() throws SQLException;

  void executeBatch() throws SQLException;
}
