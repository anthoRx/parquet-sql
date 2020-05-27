package org.arx.parquet.sql.converter;

import junit.framework.TestCase;
import org.apache.parquet.schema.PrimitiveType;
import org.arx.parquet.sql.model.SQLMetaField;


public class SqlTypeMappingTest extends TestCase {

  SQLMetaField id = new SQLMetaField("ID", 2, true, 20, 0, "java.math.BigDecimal");

  public void testGetPrimitiveType() throws ConvertException {
    PrimitiveType result = SqlTypeMapping.getPrimitiveType(id);
    System.out.println(result);
  }
}