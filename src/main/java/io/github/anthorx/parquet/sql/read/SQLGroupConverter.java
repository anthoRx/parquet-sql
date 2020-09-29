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

import io.github.anthorx.parquet.sql.read.converter.*;
import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.write.SQLWriteSupport;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.function.Consumer;

public class SQLGroupConverter extends GroupConverter {

  private static final Logger LOG = LoggerFactory.getLogger(SQLGroupConverter.class);

  private Record currentSQLRowRead;
  private final Converter[] converters;

  public SQLGroupConverter(MessageType parquetSchema) {
    this.currentSQLRowRead = new Record();
    this.converters = new Converter[parquetSchema.getFieldCount()];

    int parquetFieldIndex = 0;

    for (Type parquetField : parquetSchema.getFields()) {
      converters[parquetFieldIndex++] = getConverterFromField(parquetField);
    }
  }

  public Record getCurrentSQLRowRead() {
    return this.currentSQLRowRead;
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    LOG.debug("Start converting a new row with " + this.getClass().getSimpleName());
    currentSQLRowRead = new Record();
  }

  @Override
  public void end() {
    LOG.debug("End converting a row with " + this.getClass().getSimpleName());
  }

  /**
   * Find the right converter according to the primitive and logical type of the parquet field.
   * If no converter found for the field's type, a Runtime exception is thrown because it will be impossible
   * to read the parquet file.
   *
   * @param parquetField
   * @return
   */
  private Converter getConverterFromField(Type parquetField) {
    LogicalTypeAnnotation logicalTypeAnnotation = parquetField.getLogicalTypeAnnotation();
    String fieldName = parquetField.getName();
    Optional<Converter> converter;

    if (logicalTypeAnnotation == null) {
      converter = getConverterFromPrimitiveType(parquetField, fieldName);
    } else {
      converter = getConverterFromLogicalType(parquetField, logicalTypeAnnotation, fieldName);
    }

    return converter.orElseThrow(() -> new RuntimeException("No converter found for parquet field " + parquetField));
  }

  /**
   * Find the right converter according to a logical type.
   * Not all logical types are supported.
   *
   * @param parquetField
   * @param fieldName
   * @return
   */
  private Optional<Converter> getConverterFromLogicalType(Type parquetField, LogicalTypeAnnotation logicalTypeAnnotation, String fieldName) {
    return logicalTypeAnnotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation logicalType) {
        Consumer<RecordField<String>> f = (RecordField<String> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        return Optional.of(new FieldStringConverter(f, fieldName));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalType) {
        Consumer<RecordField<Timestamp>> f = (RecordField<Timestamp> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        return Optional.of(new FieldTimestampConverter(f, fieldName));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation logicalType) {
        Consumer<RecordField<Date>> f = (RecordField<Date> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        return Optional.of(new FieldDateConverter(f, fieldName));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation logicalType) {
        if (logicalType.getBitWidth() == 64) {
          Consumer<RecordField<Long>> f = (RecordField<Long> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
          return Optional.of(new FieldLongConverter(f, fieldName));
        } else {
          Consumer<RecordField<Integer>> f = (RecordField<Integer> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
          return Optional.of(new FieldIntegerConverter(f, fieldName));
        }
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation logicalType) {
        Consumer<RecordField<BigDecimal>> f = (RecordField<BigDecimal> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        return Optional.of(new FieldDecimalConverter(f, fieldName, parquetField.asPrimitiveType()));
      }
    });
  }

  /**
   * Find the right converter according to the primitive type.
   *
   * @param parquetField
   * @param fieldName
   * @return
   */
  private Optional<Converter> getConverterFromPrimitiveType(Type parquetField, String fieldName) {
    PrimitiveType primitiveType = parquetField.asPrimitiveType();
    Optional<Converter> converter = Optional.empty();

    switch (primitiveType.getPrimitiveTypeName()) {
      case DOUBLE:
        Consumer<RecordField<Double>> fDouble = (RecordField<Double> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        converter = Optional.of(new FieldDoubleConverter(fDouble, fieldName));
        break;
      case FLOAT:
        Consumer<RecordField<Float>> fFloat = (RecordField<Float> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        converter = Optional.of(new FieldFloatConverter(fFloat, fieldName));
        break;
      case BOOLEAN:
        Consumer<RecordField<Boolean>> fBool = (RecordField<Boolean> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        converter = Optional.of(new FieldBooleanConverter(fBool, fieldName));
        break;
      case INT32:
        Consumer<RecordField<Integer>> fint = (RecordField<Integer> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        converter = Optional.of(new FieldIntegerConverter(fint, fieldName));
        break;
      case INT64:
        Consumer<RecordField<Long>> flong = (RecordField<Long> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        converter = Optional.of(new FieldLongConverter(flong, fieldName));
        break;
      // According to Spark's code (SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE and ParquetWriteSupport), default Parquet format used for timestamp is INT96
      // INT96 is not standard and is only used for timestamp
      // Still the case on Spark 3.0.0
      case INT96:
        Consumer<RecordField<Timestamp>> fTsp = (RecordField<Timestamp> recordField) -> SQLGroupConverter.this.currentSQLRowRead.addField(recordField);
        converter = Optional.of(new FieldTimestampConverter(fTsp, fieldName));
        break;
    }

    return converter;
  }
}
