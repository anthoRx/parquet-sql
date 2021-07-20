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

package io.github.anthorx.parquet.sql.parquet.read.converter;

import io.github.anthorx.parquet.sql.jdbc.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * Converter for timestamp values.
 * Logical Types : Timestamp
 * Primitive types : INT64, INT96
 */
public class FieldTimestampConverter extends FieldConverter<Timestamp> {

  // Ref : org.apache.spark.sql.catalyst.util.DateTimeUtils
  // Ref : http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
  final int JULIAN_DAY_OF_EPOCH = 2440588;
  final long SECONDS_PER_DAY = 60 * 60 * 24L;
  final long MILLIS_PER_SECOND = 1000L;
  final long NANOS_PER_MILLIS = 1000000L;

  public FieldTimestampConverter(Consumer<ParquetRecordField<Timestamp>> f, String fieldName) {
    super(f, fieldName, ReadRecordConsumer::setTimestamp);
  }

  /**
   * @param value value to set
   */
  @Override
  public void addLong(long value) {
    acceptNewReadRecordFromValue(new Timestamp(value));
  }


  /**
   * INT96 Parquet physical format use first 8 bytes to store nanoseconds from midnight
   * and last 4 bytes to store julian days
   * Subtract offset from UTC because Timestamp constructor will add the offset according to the local timezone.
   */
  @Override
  public void addBinary(Binary value) {
    ByteBuffer buf = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    long timeOfDayNanos = buf.getLong();
    int julianDay = buf.getInt();

    long rawTimeInMillis = julianDayToMilliSeconds(julianDay) + timeOfDayNanos / NANOS_PER_MILLIS;
    long offset = TimeZone.getDefault().getOffset(rawTimeInMillis);
    acceptNewReadRecordFromValue(new Timestamp(rawTimeInMillis - offset));
  }

  private long julianDayToMilliSeconds(long day) {
    return (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND;
  }
}