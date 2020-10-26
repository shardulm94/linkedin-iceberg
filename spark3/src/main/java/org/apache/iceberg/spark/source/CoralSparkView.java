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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.types.StructType;


public class CoralSparkView implements View {

  String sql;
  List<String> udfDependenciesURLs;
  List<UDF> udfs;
  StructType viewSchema;
  String catalogName;
  String[] namespace;

  public CoralSparkView(String sql, List<String> udfDependenciesURLs, List<UDF> udfs, StructType viewSchema,
      String catalogName, String[] namespace) {
    this.sql = sql;
    this.udfDependenciesURLs = udfDependenciesURLs;
    this.udfs = udfs;
    this.viewSchema = viewSchema;
    this.catalogName = catalogName;
    this.namespace = namespace;
  }

  public static class UDF {
    String name;
    String type;
    String className;

    public UDF(String name, String type, String className) {
      this.name = name;
      this.type = type;
      this.className = className;
    }
  }

  @Override
  public String sql() {
    return sql;
  }

  @Override
  public StructType schema() {
    return viewSchema;
  }

  @Override
  public String currentCatalog() {
    return catalogName;
  }

  @Override
  public String[] currentNamespace() {
    return namespace;
  }

  @Override
  public Map<String, String> properties() {
    return Maps.newHashMap();
  }
}
