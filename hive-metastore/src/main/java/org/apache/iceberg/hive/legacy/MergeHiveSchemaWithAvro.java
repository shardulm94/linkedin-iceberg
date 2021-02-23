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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A {@link HiveSchemaWithPartnerVisitor} which augments a Hive schema with extra metadata from a partner Avro schema
 * and generates a resultant "merged" Avro schema
 *
 * 1. Fields are matched between Hive and Avro schemas using a case insensitive search by field name
 * 2. Copies field names, nullability, default value, field props from the Avro schema
 * 3. Copies field type from the Hive schema.
 *    TODO: We should also handle some cases of type promotion where the types in Avro are potentially more correct
 *    e.g.BINARY in Hive -> FIXED in Avro, STRING in Hive -> ENUM in Avro, etc
 * 4. Retains fields found only in the Hive schema; Ignores fields found only in the Avro schema
 * 5. Fields found only in Hive schema are represented as optional fields in the resultant Avro schema
 * 6. For fields found only in Hive schema, field names are sanitized to make them compatible with Avro identifier spec
 */
class MergeHiveSchemaWithAvro extends HiveSchemaWithPartnerVisitor<Schema, Schema.Field, Schema, Schema.Field> {

  static Schema visit(StructTypeInfo typeInfo, Schema schema) {
    return HiveSchemaWithPartnerVisitor.visit(typeInfo, schema, new MergeHiveSchemaWithAvro(),
        AvroPartnerAccessor.INSTANCE);
  }

  private final AtomicInteger recordCounter = new AtomicInteger(0);
  private final HiveTypeToAvroType hiveToAvro = new HiveTypeToAvroType();

  @Override
  public Schema struct(StructTypeInfo struct, Schema partner, List<Schema.Field> fieldResults) {
    boolean shouldResultBeOptional = partner == null || AvroSchemaUtil.isOptionSchema(partner);
    Schema result;
    if (partner == null || extractIfOption(partner).getType() != Schema.Type.RECORD) {
      // if there was no matching Avro struct, return a struct with new record/namespace
      int recordNum = recordCounter.incrementAndGet();
      result = Schema.createRecord("record" + recordNum, null, "namespace" + recordNum, false, fieldResults);
    } else {
      result = AvroSchemaUtil.copyRecord(extractIfOption(partner), fieldResults, null);
    }
    return shouldResultBeOptional ? AvroSchemaUtil.toOption(result) : result;
  }

  @Override
  public Schema.Field field(String name, TypeInfo field, Schema.Field partner, Schema fieldResult) {
    // No need to infer `shouldResultBeOptional`. We expect other visitor methods to return optional schemas
    // in their field results if required
    if (partner == null) {
      // if there was no matching Avro field, use name form the Hive schema and set a null default
      return new Schema.Field(
          AvroSchemaUtil.makeCompatibleName(name), fieldResult, null, Schema.Field.NULL_DEFAULT_VALUE);
    } else {
      // TODO: How to ensure that field default value is compatible with new field type generated from Hive?
      // Copy field type from the visitor result, copy everything else from the partner
      // Avro requires the default value to match the first type in the option, reorder option if required
      Schema reordered = reorderOptionIfRequired(fieldResult, partner.defaultVal());
      return AvroSchemaUtil.copyField(partner, reordered, partner.name());
    }
  }

  /**
   * Reorders an option schema so that the type of the provided default value is the first type in the option schema
   *
   * e.g. If the schema is (NULL, INT) and the default value is 1, the returned schema is (INT, NULL)
   * If the schema is not an option schema or if there is no default value, schema is returned as-is
   */
  private Schema reorderOptionIfRequired(Schema schema, Object defaultValue) {
    if (AvroSchemaUtil.isOptionSchema(schema) && defaultValue != null) {
      boolean isNullFirstOption = schema.getTypes().get(0).getType() == Schema.Type.NULL;
      if (isNullFirstOption && defaultValue.equals(JsonProperties.NULL_VALUE)) {
        return schema;
      } else {
        return Schema.createUnion(schema.getTypes().get(1), schema.getTypes().get(0));
      }
    } else {
      return schema;
    }
  }

  @Override
  public Schema list(ListTypeInfo list, Schema partner, Schema elementResult) {
    // if there was no matching Avro list, or if matching Avro list was an option, return an optional list
    boolean shouldResultBeOptional = partner == null || AvroSchemaUtil.isOptionSchema(partner);
    Schema result = Schema.createArray(elementResult);
    return shouldResultBeOptional ? AvroSchemaUtil.toOption(result) : result;
  }

  @Override
  public Schema map(MapTypeInfo map, Schema partner, Schema keyResult, Schema valueResult) {
    Preconditions.checkArgument(extractIfOption(keyResult).getType() == Schema.Type.STRING,
        "Map keys should always be non-nullable strings. Found: %s", keyResult);
    // if there was no matching Avro map, or if matching Avro map was an option, return an optional map
    boolean shouldResultBeOptional = partner == null || AvroSchemaUtil.isOptionSchema(partner);
    Schema result = Schema.createMap(valueResult);
    return shouldResultBeOptional ? AvroSchemaUtil.toOption(result) : result;
  }

  @Override
  public Schema primitive(PrimitiveTypeInfo primitive, Schema partner) {
    boolean shouldResultBeOptional = partner == null || AvroSchemaUtil.isOptionSchema(partner);
    Schema hivePrimitive = HiveTypeUtil.visit(primitive, hiveToAvro);
    // if there was no matching Avro primitive, use the Hive primitive
    Schema result = partner == null ? hivePrimitive : checkCompatibilityAndPromote(hivePrimitive, partner);
    return shouldResultBeOptional ? AvroSchemaUtil.toOption(result) : result;
  }

  private Schema checkCompatibilityAndPromote(Schema schema, Schema partner) {
    // TODO: Check if schema is compatible with partner
    //       Also do type promotion if required, schema = string & partner = enum, schema = bytes & partner = fixed, etc
    return schema;
  }

  /**
   * A {@link PartnerAccessors} which matches the requested field from a partner Avro struct by case insensitive
   * field name match
   */
  private static class AvroPartnerAccessor implements PartnerAccessors<Schema, Schema.Field> {
    private static final AvroPartnerAccessor INSTANCE = new AvroPartnerAccessor();

    private static final Schema MAP_KEY = Schema.create(Schema.Type.STRING);

    @Override
    public Schema.Field fieldPartner(Schema partner, String fieldName) {
      Schema schema = extractIfOption(partner);
      return (schema.getType() == Schema.Type.RECORD) ? findCaseInsensitive(schema, fieldName) : null;
    }

    @Override
    public Schema fieldType(Schema.Field partnerField) {
      return partnerField.schema();
    }

    @Override
    public Schema mapKeyPartner(Schema partner) {
      Schema schema = extractIfOption(partner);
      return (schema.getType() == Schema.Type.MAP) ? MAP_KEY : null;
    }

    @Override
    public Schema mapValuePartner(Schema partner) {
      Schema schema = extractIfOption(partner);
      return (schema.getType() == Schema.Type.MAP) ? schema.getValueType() : null;
    }

    @Override
    public Schema listElementPartner(Schema partner) {
      Schema schema = extractIfOption(partner);
      return (schema.getType() == Schema.Type.ARRAY) ? schema.getElementType() : null;
    }

    private Schema.Field findCaseInsensitive(Schema struct, String fieldName) {
      Preconditions.checkArgument(struct.getType() == Schema.Type.RECORD);
      // TODO: Optimize? This will be called for every struct field, we will run the for loop for every struct field
      for (Schema.Field field : struct.getFields()) {
        if (field.name().equalsIgnoreCase(fieldName)) {
          return field;
        }
      }
      return null;
    }
  }

  private static Schema extractIfOption(Schema schema) {
    if (AvroSchemaUtil.isOptionSchema(schema)) {
      return AvroSchemaUtil.fromOption(schema);
    } else {
      return schema;
    }
  }
}
