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

package org.apache.iceberg.spark;

import com.linkedin.coral.hive.hive2rel.HiveMscAdapter;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.schema.avro.ViewToAvroSchemaConverter;
import com.linkedin.coral.spark.CoralSpark;
import com.linkedin.coral.spark.containers.SparkUDFInfo;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.legacy.LegacyHiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.source.CoralSparkView;
import org.apache.iceberg.spark.source.CoralSparkView.UDF;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange;
import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty;
import org.apache.spark.sql.connector.catalog.TableChange.SetProperty;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.thrift.TException;



/**
 * A Spark TableCatalog implementation that wraps an Iceberg {@link Catalog}.
 * <p>
 * This supports the following catalog configuration options:
 * <ul>
 *   <li><tt>type</tt> - catalog type, "hive" or "hadoop"</li>
 *   <li><tt>uri</tt> - the Hive Metastore URI (Hive catalog only)</li>
 *   <li><tt>warehouse</tt> - the warehouse path (Hadoop catalog only)</li>
 *   <li><tt>default-namespace</tt> - a namespace to use as the default</li>
 * </ul>
 * <p>
 * To use a custom catalog that is not a Hive or Hadoop catalog, extend this class and override
 * {@link #buildIcebergCatalog(String, CaseInsensitiveStringMap)}.
 */
public class SparkCatalog implements StagingTableCatalog, org.apache.spark.sql.connector.catalog.SupportsNamespaces,
                                     ViewCatalog {
  private static final Set<String> DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER);

  private String catalogName = null;
  private Catalog icebergCatalog = null;
  private Catalog hiveCatalog = null;
  private boolean cacheEnabled = true;
  private SupportsNamespaces asNamespaceCatalog = null;
  private String[] defaultNamespace = null;
  private IMetaStoreClient hiveClient = null;

  /**
   * Build an Iceberg {@link Catalog} to be used by this Spark catalog adapter.
   *
   * @param name Spark's catalog name
   * @param options Spark's catalog options
   * @return an Iceberg catalog
   */
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkSession.active().sessionState().newHadoopConf();
    String catalogType = options.getOrDefault("type", "hive");
    switch (catalogType) {
      case "hive":
        int clientPoolSize = options.getInt("clients", 2);
        String uri = options.get("uri");
        return new HiveCatalog(name, uri, clientPoolSize, conf);

      case "hadoop":
        String warehouseLocation = options.get("warehouse");
        return new HadoopCatalog(name, conf, warehouseLocation);

      default:
        throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
    }
  }

  protected Catalog buildHiveCatalog(String name, CaseInsensitiveStringMap options) {
    Configuration conf = SparkSession.active().sessionState().newHadoopConf();
    int clientPoolSize = options.getInt("clients", 2);
    String uri = options.get("uri");
    return new LegacyHiveCatalog(name, uri, clientPoolSize, conf);
  }

  /**
   * Build an Iceberg {@link TableIdentifier} for the given Spark identifier.
   *
   * @param identifier Spark's identifier
   * @return an Iceberg identifier
   */
  protected TableIdentifier buildIdentifier(Identifier identifier) {
    return TableIdentifier.of(Namespace.of(identifier.namespace()), identifier.name());
  }

  @Override
  public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
    try {
      Table hiveTable = hiveCatalog.loadTable(buildIdentifier(ident));
      String tableType = hiveTable.properties().getOrDefault("table_type", "");
      Table finalTable;
      if ("iceberg".equals(tableType.toLowerCase())) {
        finalTable = icebergCatalog.loadTable(buildIdentifier(ident));
      } else {
        finalTable = hiveTable;
      }
      return new SparkTable(finalTable, !cacheEnabled);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    } catch (RuntimeException e) {
      if (e.getCause() != null && e.getCause().getClass().equals(NoSuchObjectException.class)) {
        throw new NoSuchTableException(ident);
      } else {
        throw e;
      }
    }
  }

  @Override
  public SparkTable createTable(Identifier ident, StructType schema,
                                Transform[] transforms,
                                Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      Table icebergTable = icebergCatalog.createTable(
          buildIdentifier(ident),
          icebergSchema,
          Spark3Util.toPartitionSpec(icebergSchema, transforms),
          properties.get("location"),
          Spark3Util.rebuildCreateProperties(properties));
      return new SparkTable(icebergTable, !cacheEnabled);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] transforms,
                                 Map<String, String> properties) throws TableAlreadyExistsException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      return new StagedSparkTable(icebergCatalog.newCreateTableTransaction(
          buildIdentifier(ident),
          icebergSchema,
          Spark3Util.toPartitionSpec(icebergSchema, transforms),
          properties.get("location"),
          Spark3Util.rebuildCreateProperties(properties)));
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] transforms,
                                  Map<String, String> properties) throws NoSuchTableException {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    try {
      return new StagedSparkTable(icebergCatalog.newReplaceTableTransaction(
          buildIdentifier(ident),
          icebergSchema,
          Spark3Util.toPartitionSpec(icebergSchema, transforms),
          properties.get("location"),
          Spark3Util.rebuildCreateProperties(properties),
          false /* do not create */));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] transforms,
                                          Map<String, String> properties) {
    Schema icebergSchema = SparkSchemaUtil.convert(schema);
    return new StagedSparkTable(icebergCatalog.newReplaceTableTransaction(
        buildIdentifier(ident),
        icebergSchema,
        Spark3Util.toPartitionSpec(icebergSchema, transforms),
        properties.get("location"),
        Spark3Util.rebuildCreateProperties(properties),
        true /* create or replace */));
  }

  @Override
  public SparkTable alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    SetProperty setLocation = null;
    SetProperty setSnapshotId = null;
    SetProperty pickSnapshotId = null;
    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();

    for (TableChange change : changes) {
      if (change instanceof SetProperty) {
        SetProperty set = (SetProperty) change;
        if (TableCatalog.PROP_LOCATION.equalsIgnoreCase(set.property())) {
          setLocation = set;
        } else if ("current-snapshot-id".equalsIgnoreCase(set.property())) {
          setSnapshotId = set;
        } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(set.property())) {
          pickSnapshotId = set;
        } else {
          propertyChanges.add(set);
        }
      } else if (change instanceof RemoveProperty) {
        propertyChanges.add(change);
      } else if (change instanceof ColumnChange) {
        schemaChanges.add(change);
      } else {
        throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
      }
    }

    try {
      Table table = icebergCatalog.loadTable(buildIdentifier(ident));
      commitChanges(table, setLocation, setSnapshotId, pickSnapshotId, propertyChanges, schemaChanges);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }

    return null;
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      return icebergCatalog.dropTable(buildIdentifier(ident), true);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    try {
      icebergCatalog.renameTable(buildIdentifier(from), buildIdentifier(to));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(from);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    try {
      icebergCatalog.loadTable(buildIdentifier(ident)).refresh();
    } catch (org.apache.iceberg.exceptions.NoSuchTableException ignored) {
      // ignore if the table doesn't exist, it is not cached
    }
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    return icebergCatalog.listTables(Namespace.of(namespace)).stream()
        .map(ident -> Identifier.of(ident.namespace().levels(), ident.name()))
        .toArray(Identifier[]::new);
  }

  @Override
  public String[] defaultNamespace() {
    if (defaultNamespace != null) {
      return defaultNamespace;
    }

    return new String[0];
  }

  @Override
  public String[][] listNamespaces() {
    if (asNamespaceCatalog != null) {
      return asNamespaceCatalog.listNamespaces().stream()
          .map(Namespace::levels)
          .toArray(String[][]::new);
    }

    return new String[0][];
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.listNamespaces(Namespace.of(namespace)).stream()
            .map(Namespace::levels)
            .toArray(String[][]::new);
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.loadNamespaceMetadata(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    throw new NoSuchNamespaceException(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    if (asNamespaceCatalog != null) {
      try {
        if (asNamespaceCatalog instanceof HadoopCatalog && DEFAULT_NS_KEYS.equals(metadata.keySet())) {
          // Hadoop catalog will reject metadata properties, but Spark automatically adds "owner".
          // If only the automatic properties are present, replace metadata with an empty map.
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), ImmutableMap.of());
        } else {
          asNamespaceCatalog.createNamespace(Namespace.of(namespace), metadata);
        }
      } catch (AlreadyExistsException e) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
    } else {
      throw new UnsupportedOperationException("Namespaces are not supported by catalog: " + catalogName);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();
      for (NamespaceChange change : changes) {
        if (change instanceof NamespaceChange.SetProperty) {
          NamespaceChange.SetProperty set = (NamespaceChange.SetProperty) change;
          updates.put(set.property(), set.value());
        } else if (change instanceof NamespaceChange.RemoveProperty) {
          removals.add(((NamespaceChange.RemoveProperty) change).property());
        } else {
          throw new UnsupportedOperationException("Cannot apply unknown namespace change: " + change);
        }
      }

      try {
        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(Namespace.of(namespace), updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(Namespace.of(namespace), removals);
        }

      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    } else {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    if (asNamespaceCatalog != null) {
      try {
        return asNamespaceCatalog.dropNamespace(Namespace.of(namespace));
      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        throw new NoSuchNamespaceException(namespace);
      }
    }

    return false;
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    this.cacheEnabled = Boolean.parseBoolean(options.getOrDefault("cache-enabled", "true"));
    Catalog catalog = buildIcebergCatalog(name, options);

    this.catalogName = name;
    this.icebergCatalog = cacheEnabled ? CachingCatalog.wrap(catalog) : catalog;
    this.hiveCatalog = buildHiveCatalog(name, options);
    if (catalog instanceof SupportsNamespaces) {
      this.asNamespaceCatalog = (SupportsNamespaces) catalog;
      if (options.containsKey("default-namespace")) {
        this.defaultNamespace = Splitter.on('.')
            .splitToList(options.get("default-namespace"))
            .toArray(new String[0]);
      }
    }
    try {
      this.hiveClient = new HiveMetaStoreClient(new HiveConf());
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  private static void commitChanges(Table table, SetProperty setLocation, SetProperty setSnapshotId,
                                    SetProperty pickSnapshotId, List<TableChange> propertyChanges,
                                    List<TableChange> schemaChanges) {
    // don't allow setting the snapshot and picking a commit at the same time because order is ambiguous and choosing
    // one order leads to different results
    Preconditions.checkArgument(setSnapshotId == null || pickSnapshotId == null,
        "Cannot set the current the current snapshot ID and cherry-pick snapshot changes");

    if (setSnapshotId != null) {
      long newSnapshotId = Long.parseLong(setSnapshotId.value());
      table.manageSnapshots().setCurrentSnapshot(newSnapshotId).commit();
    }

    // if updating the table snapshot, perform that update first in case it fails
    if (pickSnapshotId != null) {
      long newSnapshotId = Long.parseLong(pickSnapshotId.value());
      table.manageSnapshots().cherrypick(newSnapshotId).commit();
    }

    Transaction transaction = table.newTransaction();

    if (setLocation != null) {
      transaction.updateLocation()
          .setLocation(setLocation.value())
          .commit();
    }

    if (!propertyChanges.isEmpty()) {
      Spark3Util.applyPropertyChanges(transaction.updateProperties(), propertyChanges).commit();
    }

    if (!schemaChanges.isEmpty()) {
      Spark3Util.applySchemaChanges(transaction.updateSchema(), schemaChanges).commit();
    }

    transaction.commitTransaction();
  }

  @Override
  public Identifier[] listViews(String... namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    String db = ident.namespace()[0];
    String tbl = ident.name();
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable = hiveClient.getTable(db, tbl);
      if (!hiveTable.getTableType().toUpperCase().equals("VIRTUAL_VIEW")) {
        throw new NoSuchViewException(ident);
      }
    } catch (NoSuchObjectException e) {
      throw new NoSuchViewException(ident);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    CoralSpark coralSpark = CoralSpark.create(
        HiveToRelConverter.create(new HiveMscAdapter(hiveClient)).convertView(db, tbl)
    );
//    StructType coralSchema =  inferViewSchemaFromBaseTables(db, tbl);
    StructType coralSchema = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("intCol", DataTypes.IntegerType, false),
        DataTypes.createStructField("structCol", DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("doubleCol", DataTypes.DoubleType, false),
            DataTypes.createStructField("stringCol", DataTypes.StringType, false),
        }), false)
    });
    List<SparkUDFInfo> coralSparkUdfInfoList = coralSpark.getSparkUDFInfoList();
    return new CoralSparkView(
        coralSpark.getSparkSql(),
        coralSparkUdfInfoList.stream()
            .flatMap(udf -> udf.getArtifactoryUrls().stream())
            .map(java.net.URI::toString).distinct()
            .collect(Collectors.toList()),
        coralSparkUdfInfoList.stream().map(udf ->
            new UDF(udf.getFunctionName(), udf.getUdfType().name(), udf.getClassName()))
            .collect(Collectors.toList()),
        coralSchema,
        name(),
        ident.namespace()
    );
  }

  private StructType inferViewSchemaFromBaseTables(String db, String table) {
    HiveMscAdapter hiveMscAdapter = new HiveMscAdapter(hiveClient);
    ViewToAvroSchemaConverter viewToSchemaConverter = ViewToAvroSchemaConverter.create(hiveMscAdapter);
    org.apache.avro.Schema schema = viewToSchemaConverter.toAvroSchema(db, table);
    // NOTE: Explicitly make the struct nullable, else Spark Analyzer fails as it gets
    // nullable fields from base table relations and CAST from nullable to non-nullable fails
    // struct.asNullable <- DISABLE FOR NOW, see if it works without this in Spark 3
    return SparkSchemaUtil.convert(AvroSchemaUtil.toIceberg(schema));
  }

  @Override
  public void createView(Identifier ident, String sql, StructType schema, String[] catalogAndNamespace,
      Map<String, String> properties) throws ViewAlreadyExistsException, NoSuchNamespaceException {

  }

  @Override
  public void alterView(Identifier ident, ViewChange... changes) throws NoSuchViewException, IllegalArgumentException {

  }

  @Override
  public boolean dropView(Identifier ident) {
    return false;
  }

  @Override
  public void renameView(Identifier oldIdent, Identifier newIdent)
      throws NoSuchViewException, ViewAlreadyExistsException {

  }
}
