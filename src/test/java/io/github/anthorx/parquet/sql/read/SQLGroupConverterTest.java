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

package io.github.anthorx.parquet.sql.read;

import io.github.anthorx.parquet.sql.read.converter.*;
import junit.framework.Assert;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import static org.apache.parquet.schema.LogicalTypeAnnotation.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

public class SQLGroupConverterTest {


  @Test
  public void testPrimitiveTypeConverters() {
    MessageType mt = new MessageType("test",
        new PrimitiveType(OPTIONAL, DOUBLE, "a"),
        new PrimitiveType(OPTIONAL, FLOAT, "b"),
        new PrimitiveType(OPTIONAL, BOOLEAN, "c"),
        new PrimitiveType(OPTIONAL, INT96, "d"));

    SQLGroupConverter groupConverter = new SQLGroupConverter(mt);
    Assert.assertTrue(groupConverter.getConverter(0) instanceof FieldDoubleConverter);
    Assert.assertTrue(groupConverter.getConverter(1) instanceof FieldFloatConverter);
    Assert.assertTrue(groupConverter.getConverter(2) instanceof FieldBooleanConverter);
    Assert.assertTrue(groupConverter.getConverter(3) instanceof FieldTimestampConverter);
  }

  @Test
  public void testStringLogicalTypeConverter() {
    MessageType mt = Types.buildMessage()
        .addField(Types.optional(BINARY).as(stringType()).named("a"))
        .named("test");

    Assert.assertTrue(new SQLGroupConverter(mt).getConverter(0) instanceof FieldStringConverter);
  }

  @Test
  public void testTimestampLogicalTypeConverter() {
    MessageType mt = Types.buildMessage()
        .addField(Types.optional(INT64).as(timestampType(true, TimeUnit.MILLIS)).named("a"))
        .named("test");

    Assert.assertTrue(new SQLGroupConverter(mt).getConverter(0) instanceof FieldTimestampConverter);
  }

  @Test
  public void testDateLogicalTypeConverter() {
    MessageType mt = Types.buildMessage()
        .addField(Types.optional(INT32).as(dateType()).named("a"))
        .named("test");

    Assert.assertTrue(new SQLGroupConverter(mt).getConverter(0) instanceof FieldDateConverter);
  }

  @Test
  public void testIntLogicalTypeConverter() {
    MessageType mtInt = Types.buildMessage()
        .addField(Types.optional(INT32).as(intType(8, true)).named("a"))
        .addField(Types.optional(INT32).as(intType(16, true)).named("b"))
        .addField(Types.optional(INT32).as(intType(32, true)).named("c"))
        .addField(Types.optional(INT32).as(intType(8, false)).named("d"))
        .addField(Types.optional(INT32).as(intType(16, false)).named("e"))
        .addField(Types.optional(INT32).as(intType(32, false)).named("f"))
        .named("test");


    MessageType mtLong = Types.buildMessage()
        .addField(Types.optional(INT64).as(intType(64, true)).named("g"))
        .addField(Types.optional(INT64).as(intType(64, false)).named("g"))
        .named("test");

    SQLGroupConverter gc = new SQLGroupConverter(mtInt);
    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(gc.getConverter(i) instanceof FieldIntegerConverter);
    }

    gc = new SQLGroupConverter(mtLong);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(gc.getConverter(i) instanceof FieldLongConverter);
    }
  }

  @Test
  public void testDecimalLogicalTypeConverter() {
    MessageType mt = Types.buildMessage()
        .addField(Types.optional(INT32).as(decimalType(5, 9)).named("a"))
        .addField(Types.optional(INT64).as(decimalType(5, 18)).named("b"))
        .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(10).as(decimalType(5, 20)).named("c"))
        .addField(Types.optional(BINARY).as(decimalType(5, 38)).named("d"))
        .named("test");

    SQLGroupConverter groupConverter = new SQLGroupConverter(mt);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(groupConverter.getConverter(i) instanceof FieldDecimalConverter);
    }
  }

}
