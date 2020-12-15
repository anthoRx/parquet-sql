/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.github.anthorx.parquet.sql.read;

import io.github.anthorx.parquet.sql.utils.AssertionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class allows to lazily initialize a PreparedStatementRecordConsumer.
 */
public class RecordConsumerInitializer {

  private final static String DEFAULT_HINT = "";
  private final Connection connection;
  private final String tableName;
  private final String hint;

  public RecordConsumerInitializer(Connection connection, String tableName, String hint) {
    AssertionUtils.notNull(connection, "A valid connection is required for RecordConsumerInitializer.");
    AssertionUtils.notNull(connection, "A valid table's name is required for RecordConsumerInitializer.");
    AssertionUtils.notNull(hint, "A null hint is not allowed for RecordConsumerInitializer.");

    this.connection = connection;
    this.tableName = tableName;
    this.hint = hint;
  }

  public RecordConsumerInitializer(Connection connection, String tableName) {
    this(connection, tableName, DEFAULT_HINT);
  }

  public PreparedStatementRecordConsumer initialize(List<String> columnNames) throws SQLException, IllegalArgumentException {
    AssertionUtils.notEmpty(columnNames, "Impossible to initialize PreparedStatementRecordConsumer, no columns provided");

    String insertQuery = insertQueryBuilder(columnNames);
    PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
    return new PreparedStatementRecordConsumer(preparedStatement);
  }

  protected String insertQueryBuilder(List<String> columnNames) {
    String formattedColumns = columnNames
        .stream()
        .collect(Collectors.joining(",", "(", ")"));

    String elem = String.join(",", Collections.nCopies(columnNames.size(), "?"));

    return String.format("insert %s into %s%s values (%s)", hint, this.tableName, formattedColumns, elem);
  }
}
