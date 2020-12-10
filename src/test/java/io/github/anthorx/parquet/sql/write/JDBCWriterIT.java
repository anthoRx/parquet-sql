package io.github.anthorx.parquet.sql.write;

import io.github.anthorx.parquet.sql.read.RecordConsumerInitializer;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JDBCWriterIT {

  private final static JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:~/test", "sa", "sa");
  private static RecordConsumerInitializer recordConsumerInitializer;
  private static Connection connection;
  private static String tableName = "myTable";
  private static String column1 = "username";
  private static String column2 = "value";
  private static String column3 = "comment";

  @BeforeAll
  public static void beforeAll() throws Exception {
    recordConsumerInitializer = initConsumerInitializer();
  }

  @AfterEach
  public void cleanDatabase() throws Exception {
    connection.prepareStatement("TRUNCATE TABLE " + tableName).execute();
  }

  @AfterAll
  public static void cleanContext() throws Exception {
    connection.prepareStatement("DROP TABLE " + tableName).execute();
    connection.close();
    cp.dispose();
  }

  @Test
  public void shouldSuccessInsertDataFromFileWhenColumnsEqualsSchema() throws Exception {
    String filePath = getClass().getResource("/test/part-00001-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet").getPath();
    JDBCWriter jdbcWriter = new JDBCWriter(recordConsumerInitializer, filePath, 50);

    jdbcWriter.write();

    ResultSet result = connection.prepareStatement("SELECT * FROM " + tableName).executeQuery();
    result.next();
    Assertions.assertEquals(result.getString(column1), "Robert");
    Assertions.assertEquals(result.getInt(column2), 7);
    Assertions.assertEquals(result.getString(column3), "Testing");
  }

  @Test
  public void shouldSuccessInsertDataFromFileWhenLessColumnsThanSchema() throws Exception {
    String filePath = getClass().getResource("/test/part-00000-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet").getPath();
    JDBCWriter jdbcWriter = new JDBCWriter(recordConsumerInitializer, filePath, 50);

    jdbcWriter.write();

    ResultSet result = connection.prepareStatement("SELECT * FROM " + tableName).executeQuery();
    result.next();
    Assertions.assertEquals(result.getString(column1), "Paul");
    Assertions.assertEquals(result.getInt(column2), 4);
    Assertions.assertNull(result.getString(column3));
  }

  @Test
  public void shouldSuccessInsertDataFromFileWhenNullColumnsIsInt() throws Exception {
    String filePath = getClass().getResource("/test/part-00002-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet").getPath();
    JDBCWriter jdbcWriter = new JDBCWriter(recordConsumerInitializer, filePath, 50);

    jdbcWriter.write();

    ResultSet result = connection.prepareStatement("SELECT * FROM " + tableName).executeQuery();
    result.next();
    Assertions.assertEquals(result.getString(column1), "Patrick");
    Assertions.assertNull(result.getObject(column2));
    Assertions.assertEquals(result.getString(column3), "Null int!");
  }

  @Test
  public void shouldSuccessInsertDataFromMultipartFiles() throws Exception {
    String folderPath = getClass().getResource("/test").getPath();
    JDBCWriter jdbcWriter = new JDBCWriter(recordConsumerInitializer, folderPath, 50);

    jdbcWriter.write();

    ResultSet result = connection.prepareStatement("SELECT count(*) FROM " + tableName).executeQuery();
    result.next();
    Assertions.assertEquals(3, result.getInt(1));
  }

  @Test
  public void shouldSuccessInsertDataFromMultipartFilesInMultiThreads() throws Exception {
    String folderPath = getClass().getResource("/test").getPath();
    JDBCWriter jdbcWriter = new JDBCWriter(recordConsumerInitializer, folderPath, 50);

    jdbcWriter.write(Executors.newFixedThreadPool(4)).get();

    ResultSet result = connection.prepareStatement("SELECT count(*) FROM " + tableName).executeQuery();
    result.next();
    Assertions.assertEquals(3, result.getInt(1));
  }


  private static RecordConsumerInitializer initConsumerInitializer() throws Exception {
    connection = cp.getConnection();

    connection
        .prepareStatement(String.format("CREATE TABLE %s ( %s VARCHAR(50), %s INT, %s VARCHAR(50))", tableName, column1, column2, column3))
        .execute();

    return new RecordConsumerInitializer(cp, tableName);
  }
}
