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

package io.github.anthorx.parquet.sql.write.converter.types;

import io.github.anthorx.parquet.sql.write.converter.ConvertException;
import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.write.converter.PrimitiveTypeCreator;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.sql.SQLException;
import java.sql.Timestamp;

public class OracleTimestampConverter implements ParquetSQLConverter {

  @Override
  public boolean accept(Class<?> c) {
    return c.isAssignableFrom(oracle.sql.TIMESTAMP.class);
  }

  @Override
  public RecordField<?> convert(SQLField sqlField) throws ConvertException {
    oracle.sql.TIMESTAMP oracleTimestamp = (oracle.sql.TIMESTAMP) sqlField.getValue();
    try {
      Timestamp timestamp = oracleTimestamp.timestampValue();
      return new RecordField<>(sqlField.getName(), timestamp.getTime())
          .addWriteConsumer(RecordConsumer::addLong);
    } catch (SQLException e) {
      throw new ConvertException("Could not convert oracle.sql.TIMESTAMP", e);
    }
  }

  @Override
  public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) {
    return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
        PrimitiveType.PrimitiveTypeName.INT64,
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
        sqlColumnDefinition.isNullable());
  }
}