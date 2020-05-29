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

package io.github.anthorx.parquet.sql.model;

import org.apache.parquet.io.api.RecordConsumer;

import java.util.function.BiConsumer;

public class RecordField<T> {
  private final String name;

  private final T value;

  private final BiConsumer<RecordConsumer, T> f;

  public RecordField(String name, T value, BiConsumer<RecordConsumer, T> f) {
    this.name = name;
    this.value = value;
    this.f = f;
  }

  public String getName() {
    return name;
  }

  public T getValue() {
    return value;
  }

  public void apply(RecordConsumer r) {
    f.accept(r, value);
  }

  public boolean isNotNull() {
    return value != null;
  }
}
