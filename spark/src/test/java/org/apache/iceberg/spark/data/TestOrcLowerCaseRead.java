package org.apache.iceberg.spark.data;

import com.google.common.collect.ImmutableList;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;


public class TestOrcLowerCaseRead {


  private static TestHiveMetastore metastore;
  private static HiveClientPool clients;
  private static HiveConf hiveConf;
  private static HiveCatalog catalog;
  private static TableIdentifier currentIdentifier;

  @BeforeClass
  public static void startMetastoreAndSpark() throws Exception {
    TestOrcLowerCaseRead.metastore = new TestHiveMetastore();
    metastore.start();
    TestOrcLowerCaseRead.hiveConf = metastore.hiveConf();
    String dbPath = metastore.getDatabasePath("db");
    Database db = new Database("db", "desc", dbPath, new HashMap<>());
    TestOrcLowerCaseRead.clients = new HiveClientPool(1, hiveConf);
    clients.run(client -> {
      client.createDatabase(db);
      return null;
    });

    TestOrcLowerCaseRead.spark = SparkSession.builder()
        .master("local[2]")
        .enableHiveSupport()
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .getOrCreate();

    TestOrcLowerCaseRead.catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    catalog.close();
    TestOrcLowerCaseRead.catalog = null;
    clients.close();
    TestOrcLowerCaseRead.clients = null;
    metastore.stop();
    TestOrcLowerCaseRead.metastore = null;
    spark.stop();
    TestOrcLowerCaseRead.spark = null;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @Test
  public void writeAndValidateORCFileWithoutIds() throws Exception {
    List<Row> rows = ImmutableList.of(new GenericRow(new Object[]{1L, new GenericRow(new Object[]{"A1", "B1"})}),
        new GenericRow(new Object[]{2L, new GenericRow(new Object[]{"A2", "B2"})}));

    Dataset<Row> df = spark.createDataFrame(rows, new StructType(new StructField[]{
        DataTypes.createStructField("headerColumn", DataTypes.LongType, true),
        DataTypes.createStructField("nestedColumn", new StructType(new StructField[]{
            DataTypes.createStructField("colA", DataTypes.StringType, true),
            DataTypes.createStructField("colB", DataTypes.StringType, true)
        }), true)
    }));
    createTable("avrolowertest", ImmutableList.of(
        new FieldSchema("headerColumn", "bigint", ""),
        new FieldSchema("nestedColumn", "struct<colA:string,colB:string>", "")), ImmutableList.of(), FileFormat.AVRO);
    df.write().format("orc").mode(SaveMode.Overwrite).save("/Users/smahadik/linkedin-iceberg/spark/spark-warehouse/orclowertest/");
    df.write().format("avro").mode(SaveMode.Overwrite).save("/Users/smahadik/linkedin-iceberg/spark/spark-warehouse/avrolowertest/");

    createTable("orclowertest", ImmutableList.of(
        new FieldSchema("headerColumn", "bigint", ""),
        new FieldSchema("nestedColumn", "struct<colA:string,colB:string>", "")), ImmutableList.of(), FileFormat.ORC);
//    df.createOrReplaceTempView("tmpV");
//    spark.sql("DROP TABLE IF EXISTS default.orcLowerTest");
//    spark.sql("CREATE TABLE default.orcLowerTest STORED AS ORC AS SELECT * FROM tmpV");
//    df.write().format("orc").mode(SaveMode.Overwrite).saveAsTable("default.orcLowerTest");


    Dataset<Row> df1 = spark.read().format("iceberg").load("default.avroLowerTest");
    materialize(df1);

  }

  protected void materialize(Dataset<?> ds) {
    ds.queryExecution().toRdd().toJavaRDD().foreach(record -> { });
  }

  private static Table createTable(
      String tableName, List<FieldSchema> columns, List<FieldSchema> partitionColumns, FileFormat format)
      throws Exception {
    long currentTimeMillis = System.currentTimeMillis();
    java.nio.file.Path tableLocation = Paths.get("/Users/smahadik/linkedin-iceberg/spark/spark-warehouse/avrolowertest/");
    java.nio.file.Files.createDirectories(tableLocation);
    Table tbl = new Table(tableName,
        "default",
        System.getProperty("user.name"),
        (int) currentTimeMillis / 1000,
        (int) currentTimeMillis / 1000,
        Integer.MAX_VALUE,
        storageDescriptor(columns, tableLocation.toString(), format),
        partitionColumns,
        new HashMap<>(),
        null,
        null,
        TableType.EXTERNAL_TABLE.toString());
    tbl.getParameters().put("EXTERNAL", "TRUE");
    clients.run(c -> {
      try {
        c.createTable(tbl);
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    return tbl;
  }

  private static StorageDescriptor storageDescriptor(List<FieldSchema> columns, String location, FileFormat format) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns);
    storageDescriptor.setLocation(location);
    SerDeInfo serDeInfo = new SerDeInfo();
    switch (format) {
      case AVRO:
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        break;
      case ORC:
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private static java.nio.file.Path location(Table table) {
    return Paths.get(table.getSd().getLocation());
  }

  private static java.nio.file.Path location(Table table, List<Object> partitionValues) {
    java.nio.file.Path partitionLocation = location(table);
    for (int i = 0; i < table.getPartitionKeysSize(); i++) {
      partitionLocation = partitionLocation.resolve(
          table.getPartitionKeys().get(i).getName() + "=" + partitionValues.get(i));
    }
    return partitionLocation;
  }

}
