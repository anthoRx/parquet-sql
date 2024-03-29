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

package io.github.anthorx.parquet.sql.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

public interface ReadRecordConsumer {
  void setBoolean(boolean value);

  void setByte(byte value);

  void setShort(short value);

  void setInt(int value);

  void setLong(long value);

  void setFloat(float value);

  void setDouble(double value);

  void setBigDecimal(BigDecimal value);

  void setString(String value);

  void setBytes(byte[] value);

  void setDate(Date value);

  void setTimestamp(Timestamp value);

  void setObject(Object value);

  /** java.sql.Types id */
  void setNull(int typeId);
}
