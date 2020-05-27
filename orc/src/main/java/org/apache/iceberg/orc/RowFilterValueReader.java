/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.orc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

class RowFilterValueReader implements OrcRowReader<Object[]> {

  private final OrcValueReader<?> reader;
  private VectorizedRowBatch currentBatch;
  private StructColumnVector currentBatchColumnVector;
  private final int[] filterSchemaToReadSchemaColumnIndex;

  RowFilterValueReader(TypeDescription readSchema, TypeDescription filterSchema, Schema expectedFilterSchema) {
    this.reader = OrcSchemaWithTypeVisitor.visit(expectedFilterSchema, filterSchema, new ReadBuilder());
    filterSchemaToReadSchemaColumnIndex = buildFilterSchemaToReadSchemaColumnIndex(readSchema, filterSchema);
  }

  /**
   * For every field in the filter schema, finds the index of the field in the read schema
   */
  private int[] buildFilterSchemaToReadSchemaColumnIndex(TypeDescription readSchema, TypeDescription filterSchema) {
    //
    int[] index = new int[filterSchema.getChildren().size()];
    List<String> filterFieldNames = filterSchema.getFieldNames();
    List<String> readSchemaFieldNames = readSchema.getFieldNames();
    Map<String, Integer> readSchemaFieldNameToIndex = new HashMap<>();
    for (int i = 0; i < readSchemaFieldNames.size(); i++) {
      readSchemaFieldNameToIndex.put(readSchemaFieldNames.get(i), i);
    }
    for (int i = 0; i < filterFieldNames.size(); i++) {
      index[i] = readSchemaFieldNameToIndex.get(filterFieldNames.get(i));
    }
    return index;
  }

  /**
   * Reorders and prunes the column vectors for the read schema to match the filter schema
   */
  private ColumnVector[] readSchemaFieldsToFilterSchemaFields(ColumnVector[] source) {
    return Arrays.stream(filterSchemaToReadSchemaColumnIndex).mapToObj(idx -> source[idx]).toArray(ColumnVector[]::new);
  }

  @Override
  public Object[] read(VectorizedRowBatch batch, int row) {
    if (batch != currentBatch) {
      currentBatch = batch;
      currentBatchColumnVector = new StructColumnVector(batch.size, readSchemaFieldsToFilterSchemaFields(batch.cols));
    }
    return (Object[]) reader.read(currentBatchColumnVector, row);
  }

  private static class ReadBuilder extends OrcSchemaWithTypeVisitor<OrcValueReader<?>> {

    @Override
    public OrcValueReader<?> record(
        Types.StructType expected, TypeDescription record, List<String> names, List<OrcValueReader<?>> fields) {
      return new StructReader(fields);
    }

    @Override
    public OrcValueReader<?> list(Types.ListType iList, TypeDescription array, OrcValueReader<?> elementReader) {
      throw new IllegalArgumentException("Unhandled type " + array);
    }

    @Override
    public OrcValueReader<?> map(
        Types.MapType iMap, TypeDescription map, OrcValueReader<?> keyReader, OrcValueReader<?> valueReader) {
      throw new IllegalArgumentException("Unhandled type " + map);
    }

    @Override
    public OrcValueReader<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (primitive.getCategory()) {
        case BOOLEAN:
          return OrcValueReaders.booleans();
        case BYTE:
          // Iceberg does not have a byte type. Use int
        case SHORT:
          // Iceberg does not have a short type. Use int
        case DATE:
        case INT:
          return OrcValueReaders.ints();
        case LONG:
          return OrcValueReaders.longs();
        case FLOAT:
          return OrcValueReaders.floats();
        case DOUBLE:
          return OrcValueReaders.doubles();
        case CHAR:
        case VARCHAR:
        case STRING:
          return OrcValueReaders.strings();
        default:
          throw new IllegalArgumentException("Unhandled type " + primitive);
      }
    }
  }

  private static class StructReader extends OrcValueReaders.StructReader<Object[]> {
    private final int numFields;

    protected StructReader(List<OrcValueReader<?>> readers) {
      super(readers);
      this.numFields = readers.size();
    }

    @Override
    protected Object[] create() {
      return new Object[numFields];
    }

    @Override
    protected void set(Object[] struct, int pos, Object value) {
      struct[pos] = value;
    }
  }
}
