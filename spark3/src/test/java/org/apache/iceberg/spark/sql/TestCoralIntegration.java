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
 */

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
 */

package org.apache.iceberg.spark.sql;

import java.util.Map;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.Before;
import org.junit.Test;


public class TestCoralIntegration extends SparkCatalogTestBase {

  private static String BASE_AVRO_SCHEMA = "{    \"name\": \"Simple\",    \"type\": \"record\",    \"namespace\": \"com.linkedin.dali\",    \"fields\": [        {        \"name\": \"intCol\",        \"type\": \"int\"        },        {        \"name\": \"structCol\",        \"type\": {            \"name\": \"structCol\",            \"type\": \"record\",            \"fields\": [            {                \"name\": \"doubleCol\",                \"type\": \"double\"            },            {                \"name\": \"stringCol\",                \"type\": \"string\"            }            ]        }        }    ]}";

  public TestCoralIntegration(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE default.table (intCol int, structCol struct<doubleCol:double,stringCol:string>) STORED AS ORC TBLPROPERTIES ('write.format.default'='avro',"
        + " 'avro.schema.literal'='%s')", BASE_AVRO_SCHEMA);
    sql("INSERT INTO default.table VALUES (1, struct(1.0, 'a')), (2, struct(2.0, 'b')), (3, struct(3.0, 'c'))");
    sql("CREATE VIEW default.test_view AS SELECT * FROM default.table");
  }

  @Test
  public void testView() {
    spark.table("testhive.default.test_view").schema();
  }


}
