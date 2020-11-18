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
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class FieldConverter<T> extends PrimitiveConverter {

  protected final Consumer<RecordField<T>> f;
  protected final String fieldName;
  protected final BiConsumer<ReadRecordConsumer, T> readRecordConsumerFunction;
  protected final PrimitiveType primitiveType;

  public FieldConverter(Consumer<RecordField<T>> f, String fieldName, BiConsumer<ReadRecordConsumer, T> readRecordConsumerFunction) {
    this(f, fieldName, null, readRecordConsumerFunction);
  }

  public FieldConverter(Consumer<RecordField<T>> f, String fieldName, PrimitiveType primitiveType, BiConsumer<ReadRecordConsumer, T> readRecordConsumerFunction) {
    this.f = f;
    this.fieldName = fieldName;
    this.primitiveType = primitiveType;
    this.readRecordConsumerFunction = readRecordConsumerFunction;
  }

  protected void acceptNewReadRecordFromValue(T value) {
    RecordField<T> recordField = new RecordField<>(fieldName, value)
        .addReadConsumer(readRecordConsumerFunction);
    f.accept(recordField);
  }
}
