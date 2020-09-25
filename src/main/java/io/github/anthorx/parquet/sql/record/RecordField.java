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

package io.github.anthorx.parquet.sql.record;

import org.apache.parquet.io.api.RecordConsumer;

import java.util.function.BiConsumer;

public class RecordField<T> {
  private final String name;

  private final T value;

  // Function used to write the field somewhere
  private BiConsumer<RecordConsumer, T> writeConsumer;

  // Function used to read the field from a parquet file
  private BiConsumer<ReadRecordConsumer, T> readConsumer;

  public RecordField(String name, T value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public T getValue() {
    return value;
  }

  public RecordField<T> addWriteConsumer(BiConsumer<RecordConsumer, T> writeConsumer) {
    this.writeConsumer = writeConsumer;
    return this;
  }

  public RecordField<T> addReadConsumer(BiConsumer<ReadRecordConsumer, T> readConsumer) {
    this.readConsumer = readConsumer;
    return this;
  }

  public void applyWriteConsumer(RecordConsumer r) {
    writeConsumer.accept(r, value);
  }

  public void applyReadConsumer(ReadRecordConsumer r) {
    readConsumer.accept(r, value);
  }

  public boolean isNotNull() {
    return value != null;
  }

  @Override
  public String toString() {
    return "RecordField{" +
        "name='" + name + '\'' +
        ", value=" + value +
        '}';
  }
}
