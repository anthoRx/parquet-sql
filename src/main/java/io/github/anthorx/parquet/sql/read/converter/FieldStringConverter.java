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

package io.github.anthorx.parquet.sql.read.converter;

import io.github.anthorx.parquet.sql.record.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.parquet.io.api.Binary;

import java.util.function.Consumer;

/**
 * Converter for string values.
 * Logical Types : String
 * Primitive types : Binary
 */
public class FieldStringConverter extends FieldConverter<String> {

  public FieldStringConverter(Consumer<RecordField<String>> f, String fieldName) {
    super(f, fieldName, ReadRecordConsumer::setString);
  }

  @Override
  final public void addBinary(Binary value) {
    acceptNewReadRecordFromValue(value.toStringUsingUTF8());
  }

  @Override
  public void addLong(long value) {
    acceptNewReadRecordFromValue(Long.toString(value));
  }

  @Override
  public void addInt(int value) {
    acceptNewReadRecordFromValue(Integer.toString(value));
  }

  @Override
  public void addFloat(float value) {
    acceptNewReadRecordFromValue(Float.toString(value));
  }

  @Override
  public void addDouble(double value) {
    acceptNewReadRecordFromValue(Double.toString(value));
  }

  @Override
  public void addBoolean(boolean value) {
    acceptNewReadRecordFromValue(Boolean.toString(value));
  }
}