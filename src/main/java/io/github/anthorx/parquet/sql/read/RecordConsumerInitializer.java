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

  final private Connection connection;
  final private String tableName;

  public RecordConsumerInitializer(Connection connection, String tableName) {
    this.connection = connection;
    this.tableName = tableName;
  }

  public PreparedStatementRecordConsumer initialize(List<String> columnNames) throws SQLException, IllegalArgumentException {
    if (columnNames.isEmpty()) {
      throw new IllegalArgumentException("Impossible to initialize PreparedStatementRecordConsumer, no columns provided");
    }

    String insertQuery = insertQueryBuilder(columnNames);
    PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
    return new PreparedStatementRecordConsumer(preparedStatement);
  }

  protected String insertQueryBuilder(List<String> columnNames) {
    String formattedColumns = columnNames
        .stream()
        .collect(Collectors.joining(",", "(", ")"));

    String elem = String.join(",", Collections.nCopies(columnNames.size(), "?"));

    return String.format("insert into %s%s values (%s)", this.tableName, formattedColumns, elem);
  }
}
