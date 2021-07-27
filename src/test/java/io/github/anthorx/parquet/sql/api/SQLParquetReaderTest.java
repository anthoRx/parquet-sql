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

package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.parquet.model.Record;
import io.github.anthorx.parquet.sql.parquet.model.RecordField;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.Type;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.mockito.internal.hamcrest.HamcrestArgumentMatcher;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test a scenario case
 */
public class SQLParquetReaderTest {

  private <T> void assertField(String fieldName, Record record, T expectedValue) {
    Optional<RecordField<?>> field = record.getField(fieldName);

    assertTrue(field.isPresent());

    if (expectedValue instanceof Date) {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      String expected = df.format((Date) expectedValue);
      String actual = df.format((Date) field.get().getValue());
      assertEquals(expected, actual);
    } else {
      assertEquals(expectedValue, field.get().getValue());
    }
  }

  private SQLParquetReader parquetReader(String path) throws IOException {
    String filePath = getClass().getResource(path).getPath();

    return new SQLParquetReader(filePath, new Configuration());
  }

  @Test
  public void readingParquetFile() throws IOException {
    SQLParquetReader sqlParquetReader = parquetReader("/test.parquet");

    Record record = sqlParquetReader.read();

    assertField("timestamp", record, Timestamp.valueOf("2019-02-04 00:00:00"));
    assertField("date", record, Date.valueOf("2019-01-17"));
    assertField("bd_8_3", record, new BigDecimal("88.333").setScale(3));
    assertField("bd_10_5", record, new BigDecimal("1111.2222").setScale(5));
    assertField("bd_10_0", record, new BigDecimal("10.0").setScale(0));
    assertField("bd_20_12", record, new BigDecimal("111.9999999999").setScale(12));
    assertField("bd_38_18", record, new BigDecimal("1111.2222").setScale(18));
    assertField("float", record, new Float("11.22"));
    assertField("double", record, new Double("11.22"));
    assertField("string", record, "a string");
    assertField("int", record, 5);
    assertField("bool", record, true);
  }

  @Test
  public void readingParquetFolder() throws IOException {
    SQLParquetReader sqlParquetReader = parquetReader("/test");

    List<String> actual = Arrays.asList(
        (String) sqlParquetReader.read().getField("username").get().getValue(),
        (String) sqlParquetReader.read().getField("username").get().getValue(),
        (String) sqlParquetReader.read().getField("username").get().getValue()
    );

    assertThat(actual, containsInAnyOrder("Patrick", "Paul", "Robert"));

    // Then the last line returns null
    assertNull(sqlParquetReader.read());
  }

  @Test
  public void getSchema() throws IOException {
    SQLParquetReader sqlParquetReader = parquetReader("/test");

    assertNotNull(sqlParquetReader.getSchema());
  }

  @Test
  public void getFieldsNames() throws IOException {
    SQLParquetReader sqlParquetReader = parquetReader("/test");

    List<String> actual = sqlParquetReader.getFieldsNames();

    assertThat(actual, containsInAnyOrder("username", "value", "comment"));
  }

  @Test
  public void getFields() throws IOException {
    SQLParquetReader sqlParquetReader = parquetReader("/test");

    List<Type> actual = sqlParquetReader.getFields();

    assertNotNull(actual);
  }
}
