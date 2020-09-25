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

import java.util.Optional;

public class SQLField {

  final private String name;

  final private Object value;

  final private int sqlType;

  final private Optional<Integer> precision;

  final private Optional<Integer> scale;

  final private String columnClassName;

  public SQLField(String name, Object value) {
    this.name = name;
    this.value = value;
    this.sqlType = -1;
    this.precision = Optional.empty();
    this.scale = Optional.empty();
    this.columnClassName = "";
  }

  public SQLField(String name, Object value, int sqlType, String columnClassName) {
    this.name = name;
    this.value = value;
    this.sqlType = sqlType;
    this.columnClassName = columnClassName;
    this.precision = Optional.empty();
    this.scale = Optional.empty();
  }

  public SQLField(String name, Object value, int sqlType, int precision, int scale, String columnClassName) {
    this.name = name;
    this.value = value;
    this.sqlType = sqlType;
    this.precision = Optional.of(precision);
    this.scale = Optional.of(scale);
    this.columnClassName = columnClassName;
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return value;
  }

  public int getSqlType() {
    return sqlType;
  }

  public Optional<Integer> getPrecision() {
    return precision;
  }

  public Optional<Integer> getScale() {
    return scale;
  }

  public String getColumnClassName() {
    return columnClassName;
  }

  @Override
  public String toString() {
    return "SQLField{" +
        "name='" + name + '\'' +
        ", value=" + value +
        ", sqlType=" + sqlType +
        ", precision=" + precision +
        ", scale=" + scale +
        ", columnClassName='" + columnClassName + '\'' +
        '}';
  }
}
