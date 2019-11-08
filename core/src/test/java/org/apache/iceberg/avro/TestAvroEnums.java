package org.apache.iceberg.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Files;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestAvroEnums {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateEnums() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("enumCol")
        .type()
        .enumeration("testEnum")
        .symbols("SYMB1", "SYMB2")
        .enumDefault("SYMB2")
        .endRecord();

    Record enumRecord1 = new GenericData.Record(avroSchema);
    enumRecord1.put("enumCol", new GenericData.EnumSymbol(avroSchema.getField("enumCol").schema(), "SYMB1"));
    Record enumRecord2 = new GenericData.Record(avroSchema);
    enumRecord2.put("enumCol", new GenericData.EnumSymbol(avroSchema.getField("enumCol").schema(), "SYMB2"));
    List<Record> expected = ImmutableList.of(enumRecord1, enumRecord2);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(enumRecord1);
      writer.append(enumRecord2);
    }

    Schema schema = new Schema(AvroSchemaUtil.convert(avroSchema).asStructType().fields());
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader = Avro.read(Files.localInput(testFile)).project(schema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      Assert.assertEquals(expected.get(i).get("enumCol").toString(), rows.get(i).get("enumCol"));
    }
  }
}
