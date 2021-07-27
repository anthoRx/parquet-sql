package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.parquet.model.Record;
import org.apache.hadoop.conf.Configuration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JDBCWriterIntegrationTest {

  private final static JdbcConnectionPool dataSource = JdbcConnectionPool.create("jdbc:h2:~/test.db", "sa", "sa");
  private static Connection connection;
  private static String tableName = "myTable";
  private static String column1 = "username";
  private static String column2 = "value";
  private static String column3 = "comment";

  @BeforeAll
  public static void beforeAll() throws Exception {
    connection = dataSource.getConnection();

    connection
        .prepareStatement(String.format("CREATE TABLE %s ( %s VARCHAR(50), %s INT, %s VARCHAR(50))", tableName, column1, column2, column3))
        .execute();
  }

  @AfterEach
  public void cleanDatabase() throws Exception {
    connection.prepareStatement("TRUNCATE TABLE " + tableName).execute();
  }

  @AfterAll
  public static void cleanContext() throws Exception {
    connection.prepareStatement("DROP TABLE " + tableName).execute();
    connection.close();
    dataSource.dispose();
  }

  private String getResource(String fileName) {
    return Objects.requireNonNull(getClass().getResource(fileName)).getPath();
  }

  @Test
  public void shouldSuccessInsertDataFromFileWhenColumnsEqualsSchema() throws Exception {

    SQLParquetReader parquetReader = new SQLParquetReader(getResource("/test/part-00001-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet"), new Configuration());

    JDBCWriter jdbcWriter = new JDBCWriter(connection, tableName, parquetReader.getFieldsNames());

    writeParquetIntoTable(parquetReader, jdbcWriter);

    ResultSet result = connection.prepareStatement("SELECT * FROM " + tableName).executeQuery();
    result.next();
    assertEquals(result.getString(column1), "Robert");
    assertEquals(result.getInt(column2), 7);
    assertEquals(result.getString(column3), "Testing");
  }

  @Test
  public void shouldSuccessInsertDataFromFileWhenLessColumnsThanSchema() throws Exception {
    SQLParquetReader parquetReader = new SQLParquetReader(getResource("/test/part-00000-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet"), new Configuration());

    JDBCWriter jdbcWriter = new JDBCWriter(connection, tableName, parquetReader.getFieldsNames());

    writeParquetIntoTable(parquetReader, jdbcWriter);

    ResultSet result = connection.prepareStatement("SELECT * FROM " + tableName).executeQuery();
    result.next();
    assertEquals(result.getString(column1), "Paul");
    assertEquals(result.getInt(column2), 4);
    assertNull(result.getString(column3));
  }

  @Test
  public void shouldSuccessInsertDataFromFileWhenNullColumnsIsInt() throws Exception {
    SQLParquetReader parquetReader = new SQLParquetReader(getResource("/test/part-00002-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet"), new Configuration());

    JDBCWriter jdbcWriter = new JDBCWriter(connection, tableName, parquetReader.getFieldsNames());

    writeParquetIntoTable(parquetReader, jdbcWriter);

    ResultSet result = connection.prepareStatement("SELECT * FROM " + tableName).executeQuery();
    result.next();
    assertEquals(result.getString(column1), "Patrick");
    assertNull(result.getObject(column2));
    assertEquals(result.getString(column3), "Null int!");
  }

  private void writeParquetIntoTable(SQLParquetReader parquetReader, JDBCWriter jdbcWriter) throws IOException, SQLException {
    Record currentRecord = parquetReader.read();
    while (currentRecord != null) {
      currentRecord.readAll(parquetReader.getFields(), jdbcWriter);

      jdbcWriter.addBatch();
      currentRecord = parquetReader.read();
    }
    jdbcWriter.executeBatch();
  }
}
