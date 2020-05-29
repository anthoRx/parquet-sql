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

package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.model.RecordField;
import io.github.anthorx.parquet.sql.model.SQLField;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
public class RecordFieldConverterTest {

  RecordFieldConverter recordFieldConverter = new RecordFieldConverter();

  String defaultTimestampString = "2020-05-27 00:41:17.0";
  SQLField doubleNum = new SQLField("doubleNum", new java.math.BigDecimal("0.090672443454735077377029999820651048832"), 2, 126, -127, "java.lang.Double");
  SQLField bigDecimal = new SQLField("bigDecimal", new java.math.BigDecimal("49342"), 2, 20, 0, "java.math.BigDecimal");
  SQLField nullBigDecimal = new SQLField("nullBigDecimal", null, 2, 0, -127, "java.math.BigDecimal");
  SQLField timestamp = new SQLField("timestamp", Timestamp.valueOf(defaultTimestampString), 93, 0, 0, "java.sql.Timestamp");
  SQLField oracleTimestamp = new SQLField("oracleTimestamp", new oracle.sql.TIMESTAMP(defaultTimestampString), 93, 0, 6, "oracle.sql.TIMESTAMP");
  SQLField nullTimestamp = new SQLField("nullTimestamp", null, 93, 0, 0, "java.sql.Timestamp");
  SQLField string = new SQLField("string", "string", 12, 20, 0, "java.lang.String");
  SQLField nullString = new SQLField("nullString", null, 12, 20, 0, "java.lang.String");

  @Test
  public void testConvert_string_convertBinary() throws ConvertException {
    RecordField<Binary> result = (RecordField<Binary>) recordFieldConverter.convert(string);
    assertThat(result.getValue(), instanceOf(Binary.class));
    assertArrayEquals(((String) string.getValue()).getBytes(), result.getValue().getBytes());
  }

  @Test
  public void testConvert_nullString() throws ConvertException {
    RecordField<String> result = (RecordField<String>) recordFieldConverter.convert(nullString);
    assertEquals(null, result.getValue());
  }

  @Test
  public void testConvert_nullTimestamp() throws ConvertException {
    RecordField<Timestamp> result = (RecordField<Timestamp>) recordFieldConverter.convert(nullTimestamp);
    assertEquals(null, result.getValue());
  }

  @Test
  public void testConvert_timestamp() throws ConvertException {
    RecordField<Long> result = (RecordField<Long>) recordFieldConverter.convert(timestamp);
    assertThat(result.getValue(), instanceOf(Long.class));
    assertEquals(Timestamp.valueOf(defaultTimestampString).getTime(), result.getValue());
  }

  @Test
  public void testConvert_oracleTimestamp() throws ConvertException {
    RecordField<Long> result = (RecordField<Long>) recordFieldConverter.convert(oracleTimestamp);
    assertThat(result.getValue(), instanceOf(Long.class));
    assertEquals(Timestamp.valueOf(defaultTimestampString).getTime(), result.getValue());
  }

  @Test
  public void testConvert_double() throws ConvertException {
    RecordField<Double> result = (RecordField<Double>) recordFieldConverter.convert(doubleNum);
    assertThat(result.getValue(), instanceOf(Double.class));
    assertEquals(((BigDecimal) doubleNum.getValue()).doubleValue(), result.getValue());
  }

  @Test
  public void testConvert_precision20bigDecimal() throws ConvertException {
    RecordField<Binary> result = (RecordField<Binary>) recordFieldConverter.convert(bigDecimal);

    assertThat(result.getValue(), instanceOf(Binary.class));
    byte[] actual = result.getValue().getBytes();
    byte[] expected = ((BigDecimal) bigDecimal.getValue()).unscaledValue().toByteArray();
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testConvert_nullBigDecimal() throws ConvertException {
    RecordField<Binary> result = (RecordField<Binary>) recordFieldConverter.convert(nullBigDecimal);

    assertNull(result.getValue());
  }
}