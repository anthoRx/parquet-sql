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

package io.github.anthorx.parquet.sql.jdbc.model;

public class SQLColumnDefinition {

  private final String name;
  private final int columnType;
  private final boolean isNullable;
  private final int precision;
  private final int scale;
  private final String columnTypeName;

  public SQLColumnDefinition(String name, int columnType, boolean isNullable, int precision, int scale, String columnTypeName) {
    this.name = name;
    this.columnType = columnType;
    this.isNullable = isNullable;
    this.precision = precision;
    this.scale = scale;
    this.columnTypeName = columnTypeName;
  }

  public String getName() {
    return name;
  }

  public int getColumnType() {
    return columnType;
  }

  public boolean isNullable() {
    return isNullable;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  public String getColumnTypeName() {
    return columnTypeName;
  }
}
