/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.paimon.trino.catalog;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.security.SecurityContext;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.trino.ClassLoaderUtils;
import org.apache.paimon.utils.SnapshotNotExistException;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Trino catalog, use it after set session. */
public class TrinoCatalog implements Catalog {

    private final Options options;

    private final Configuration configuration;

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    private Catalog current;

    private volatile boolean inited = false;

    public TrinoCatalog(
            Options options,
            Configuration configuration,
            TrinoFileSystemFactory trinoFileSystemFactory) {
        this.options = options;
        this.configuration = configuration;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
    }

    public void initSession(ConnectorSession connectorSession) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    current =
                            ClassLoaderUtils.runWithContextClassLoader(
                                    () -> {
                                        TrinoFileSystem trinoFileSystem =
                                                trinoFileSystemFactory.create(connectorSession);
                                        CatalogContext catalogContext =
                                                CatalogContext.create(
                                                        options,
                                                        configuration,
                                                        // set this to null, make it back when
                                                        // update to 427 and later
                                                        null,
                                                        // new TrinoFileIOLoader(trinoFileSystem),
                                                        null);
                                        try {
                                            SecurityContext.install(catalogContext);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        return CatalogFactory.createCatalog(catalogContext);
                                    },
                                    this.getClass().getClassLoader());
                    inited = true;
                }
            }
        }
    }

    @Override
    public Map<String, String> options() {
        if (!inited) {
            throw new RuntimeException("Not inited yet.");
        }
        return current.options();
    }

    @Override
    public CatalogLoader catalogLoader() {
        return current.catalogLoader();
    }

    @Override
    public List<String> listDatabases() {
        return current.listDatabases();
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer integer, @Nullable String s, @Nullable String s1) {
        return current.listDatabasesPaged(integer, s, s1);
    }

    @Override
    public void createDatabase(String s, boolean b, Map<String, String> map)
            throws DatabaseAlreadyExistException {
        current.createDatabase(s, b, map);
    }

    @Override
    public Database getDatabase(String s) throws DatabaseNotExistException {
        return current.getDatabase(s);
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        current.dropDatabase(s, b, b1);
    }

    @Override
    public void alterDatabase(String s, List<PropertyChange> list, boolean b)
            throws DatabaseNotExistException {
        current.alterDatabase(s, list, b);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return current.getTable(identifier);
    }

    @Override
    public List<String> listTables(String s) throws DatabaseNotExistException {
        return current.listTables(s);
    }

    @Override
    public PagedList<String> listTablesPaged(
            String s,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        return current.listTablesPaged(s, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String s,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        return current.listTableDetailsPaged(s, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public void dropTable(Identifier identifier, boolean b) throws TableNotExistException {
        current.dropTable(identifier, b);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        current.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfExistsb)
            throws TableNotExistException, TableAlreadyExistException {
        current.renameTable(fromTable, toTable, ignoreIfExistsb);
    }

    @Override
    public void alterTable(Identifier identifier, List<SchemaChange> list, boolean ignoreIfExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, list, ignoreIfExists);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return current.listPartitions(identifier);
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer integer,
            @Nullable String s,
            @Nullable String s1)
            throws TableNotExistException {
        return current.listPartitionsPaged(identifier, integer, s, s1);
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return current.supportsListObjectsPaged();
    }

    @Override
    public boolean supportsVersionManagement() {
        return current.supportsVersionManagement();
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String s,
            Snapshot snapshot,
            List<PartitionStatistics> list)
            throws TableNotExistException {
        return current.commitSnapshot(identifier, s, snapshot, list);
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException {
        return current.loadSnapshot(identifier);
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String s)
            throws TableNotExistException {
        return current.loadSnapshot(identifier, s);
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer integer, @Nullable String s)
            throws TableNotExistException {
        return current.listSnapshotsPaged(identifier, integer, s);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant) throws TableNotExistException {
        current.rollbackTo(identifier, instant);
    }

    @Override
    public void createBranch(Identifier identifier, String s, @Nullable String s1)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        current.createBranch(identifier, s, s1);
    }

    @Override
    public void dropBranch(Identifier identifier, String s) throws BranchNotExistException {
        current.dropBranch(identifier, s);
    }

    @Override
    public void fastForward(Identifier identifier, String s) throws BranchNotExistException {
        current.fastForward(identifier, s);
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        return current.listBranches(identifier);
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String s)
            throws TableNotExistException, TagNotExistException {
        return current.getTag(identifier, s);
    }

    @Override
    public void createTag(
            Identifier identifier, String s, @Nullable Long aLong, @Nullable String s1, boolean b)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException {
        current.createTag(identifier, s, aLong, s1, b);
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier, @Nullable Integer integer, @Nullable String s)
            throws TableNotExistException {
        return current.listTagsPaged(identifier, integer, s);
    }

    @Override
    public void deleteTag(Identifier identifier, String s)
            throws TableNotExistException, TagNotExistException {
        current.deleteTag(identifier, s);
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> list)
            throws TableNotExistException {
        current.createPartitions(identifier, list);
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> list)
            throws TableNotExistException {
        current.dropPartitions(identifier, list);
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> list)
            throws TableNotExistException {
        current.alterPartitions(identifier, list);
    }

    @Override
    public List<String> listFunctions(String s) throws DatabaseNotExistException {
        return current.listFunctions(s);
    }

    @Override
    public Function getFunction(Identifier identifier) throws FunctionNotExistException {
        return current.getFunction(identifier);
    }

    @Override
    public void createFunction(Identifier identifier, Function function, boolean b)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        current.createFunction(identifier, function, b);
    }

    @Override
    public void dropFunction(Identifier identifier, boolean b) throws FunctionNotExistException {
        current.dropFunction(identifier, b);
    }

    @Override
    public void alterFunction(Identifier identifier, List<FunctionChange> list, boolean b)
            throws FunctionNotExistException, DefinitionAlreadyExistException,
                    DefinitionNotExistException {
        current.alterFunction(identifier, list, b);
    }

    @Override
    public List<String> authTableQuery(Identifier identifier, @Nullable List<String> list)
            throws TableNotExistException {
        return current.authTableQuery(identifier, list);
    }

    @Override
    public void close() throws Exception {
        if (current != null) {
            current.close();
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        current.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, change, ignoreIfNotExists);
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> list)
            throws TableNotExistException {
        current.markDonePartitions(identifier, list);
    }

    @Override
    public boolean caseSensitive() {
        return current.caseSensitive();
    }
}
