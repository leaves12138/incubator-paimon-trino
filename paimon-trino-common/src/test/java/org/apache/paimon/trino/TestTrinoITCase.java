/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.ZoneOffset.UTC;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for trino connector. */
public abstract class TestTrinoITCase extends AbstractTestQueryFramework {

    private static final String CATALOG = "paimon";
    private static final String DB = "default";

    protected long t2FirstCommitTimestamp;

    private int trinoVersion;

    public TestTrinoITCase(int trinoVersion) {
        this.trinoVersion = trinoVersion;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        // flink sink
        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(
                GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper1.commit();

        Path tablePath2 = new Path(warehouse, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper2.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper2.commit();
        testHelper2.createTag("1");
        t2FirstCommitTimestamp = System.currentTimeMillis();
        testHelper2.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper2.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        testHelper2.commit();
        testHelper2.createTag("tag-2");

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "pt", DataTypes.STRING()),
                                    new DataField(1, "a", new IntType()),
                                    new DataField(2, "b", new BigIntType()),
                                    new DataField(3, "c", new BigIntType()),
                                    new DataField(4, "d", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath3)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.singletonList("pt"),
                                    Collections.emptyList(),
                                    new HashMap<>(),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath3);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(fromString("1"), 1, 1L, 1L, 1));
            writer.write(GenericRow.of(fromString("1"), 1, 2L, 2L, 2));
            writer.write(GenericRow.of(fromString("2"), 3, 3L, 3L, 3));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/empty_t");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(1, "a", new IntType()),
                                    new DataField(2, "b", new BigIntType())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>(),
                                    ""));
        }

        {
            Path tablePath4 = new Path(warehouse, "default.db/t4");
            List<DataField> innerRowFields = new ArrayList<>();
            innerRowFields.add(new DataField(4, "innercol1", new IntType()));
            innerRowFields.add(
                    new DataField(5, "innercol2", new VarCharType(VarCharType.MAX_LENGTH)));
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "i", new IntType()),
                                    new DataField(
                                            1,
                                            "map",
                                            new MapType(
                                                    new VarCharType(VarCharType.MAX_LENGTH),
                                                    new VarCharType(VarCharType.MAX_LENGTH))),
                                    new DataField(2, "innerrow", new RowType(true, innerRowFields)),
                                    new DataField(3, "array", new ArrayType(new IntType()))));
            new SchemaManager(LocalFileIO.create(), tablePath4)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.singletonList("i"),
                                    Collections.singletonMap("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath4);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            1,
                            new GenericMap(
                                    new HashMap<>() {
                                        {
                                            put(fromString("1"), fromString("2"));
                                        }
                                    }),
                            GenericRow.of(2, fromString("male")),
                            new GenericArray(new int[] {1, 2, 3})));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t99");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "boolean", DataTypes.BOOLEAN()),
                                    new DataField(1, "tinyint", DataTypes.TINYINT()),
                                    new DataField(2, "smallint", DataTypes.SMALLINT()),
                                    new DataField(3, "int", DataTypes.INT()),
                                    new DataField(4, "bigint", DataTypes.BIGINT()),
                                    new DataField(5, "float", DataTypes.FLOAT()),
                                    new DataField(6, "double", DataTypes.DOUBLE()),
                                    new DataField(7, "char", DataTypes.CHAR(5)),
                                    new DataField(8, "varchar", DataTypes.VARCHAR(100)),
                                    new DataField(9, "date", DataTypes.DATE()),
                                    new DataField(10, "timestamp_0", DataTypes.TIMESTAMP(0)),
                                    new DataField(11, "timestamp_3", DataTypes.TIMESTAMP(3)),
                                    new DataField(12, "timestamp_6", DataTypes.TIMESTAMP(6)),
                                    new DataField(
                                            13,
                                            "timestamp_tz",
                                            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                    new DataField(14, "decimal", DataTypes.DECIMAL(10, 5)),
                                    new DataField(15, "varbinary", DataTypes.VARBINARY(10)),
                                    new DataField(16, "array", DataTypes.ARRAY(DataTypes.INT())),
                                    new DataField(
                                            17,
                                            "map",
                                            DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                                    new DataField(
                                            18,
                                            "row",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(100, "q1", DataTypes.INT()),
                                                    DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath6)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    List.of(
                                            "boolean",
                                            "tinyint",
                                            "smallint",
                                            "int",
                                            "bigint",
                                            "float",
                                            "double",
                                            "char",
                                            "varchar",
                                            "date",
                                            "timestamp_0",
                                            "timestamp_3",
                                            "timestamp_6",
                                            "timestamp_tz",
                                            "decimal"),
                                    List.of(
                                            "boolean",
                                            "tinyint",
                                            "smallint",
                                            "int",
                                            "bigint",
                                            "float",
                                            "double",
                                            "char",
                                            "varchar",
                                            "date",
                                            "timestamp_0",
                                            "timestamp_3",
                                            "timestamp_6",
                                            "timestamp_tz",
                                            "decimal",
                                            "varbinary"),
                                    Collections.singletonMap("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath6);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            true,
                            (byte) 1,
                            (short) 1,
                            1,
                            1L,
                            1.0f,
                            1.0d,
                            BinaryString.fromString("char1"),
                            BinaryString.fromString("varchar1"),
                            0,
                            Timestamp.fromMicros(1694505288000000L),
                            Timestamp.fromMicros(1694505288001000L),
                            Timestamp.fromMicros(1694505288001001L),
                            Timestamp.fromMicros(1694505288002001L),
                            Decimal.fromUnscaledLong(10000, 10, 5),
                            new byte[] {0x01, 0x02, 0x03},
                            new GenericArray(new int[] {1, 1, 1}),
                            new GenericMap(Map.of(1, 1)),
                            GenericRow.of(1, 1)));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath7 = new Path(warehouse, "default.db/t100");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "boolean", DataTypes.BOOLEAN()),
                                    new DataField(1, "tinyint", DataTypes.TINYINT()),
                                    new DataField(2, "smallint", DataTypes.SMALLINT()),
                                    new DataField(3, "int", DataTypes.INT()),
                                    new DataField(4, "bigint", DataTypes.BIGINT()),
                                    new DataField(5, "float", DataTypes.FLOAT()),
                                    new DataField(6, "double", DataTypes.DOUBLE()),
                                    new DataField(7, "char", DataTypes.CHAR(5)),
                                    new DataField(8, "varchar", DataTypes.VARCHAR(100)),
                                    new DataField(9, "date", DataTypes.DATE()),
                                    new DataField(10, "timestamp_0", DataTypes.TIMESTAMP(3)),
                                    new DataField(11, "timestamp_3", DataTypes.TIMESTAMP(3)),
                                    new DataField(12, "timestamp_6", DataTypes.TIMESTAMP(6)),
                                    new DataField(13, "decimal", DataTypes.DECIMAL(10, 5)),
                                    new DataField(14, "varbinary", DataTypes.VARBINARY(10)),
                                    new DataField(15, "array", DataTypes.ARRAY(DataTypes.INT())),
                                    new DataField(
                                            16,
                                            "map",
                                            DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                                    new DataField(
                                            17,
                                            "row",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(100, "q1", DataTypes.INT()),
                                                    DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.singletonMap("bucket", "-1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            true,
                            (byte) 1,
                            (short) 1,
                            1,
                            1L,
                            1.0f,
                            1.0d,
                            BinaryString.fromString("char1"),
                            BinaryString.fromString("varchar1"),
                            0,
                            Timestamp.fromMicros(1694505288000000L),
                            Timestamp.fromMicros(1694505288001000L),
                            Timestamp.fromMicros(1694505288001001L),
                            Decimal.fromUnscaledLong(10000, 10, 5),
                            new byte[] {0x01, 0x02, 0x03},
                            new GenericArray(new int[] {1, 1, 1}),
                            new GenericMap(Map.of(1, 1)),
                            GenericRow.of(1, 1)));
            commit.commit(0, writer.prepareCommit(true, 0));

            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .commitChanges(SchemaChange.dropColumn("smallint"));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            true,
                            (byte) 1,
                            1,
                            1L,
                            1.0f,
                            1.0d,
                            BinaryString.fromString("char1"),
                            BinaryString.fromString("varchar1"),
                            0,
                            Timestamp.fromMicros(1694505288000000L),
                            Timestamp.fromMicros(1694505288001000L),
                            Timestamp.fromMicros(1694505288001001L),
                            Decimal.fromUnscaledLong(10000, 10, 5),
                            new byte[] {0x01, 0x02, 0x03},
                            new GenericArray(new int[] {1, 1, 1}),
                            new GenericMap(Map.of(1, 1)),
                            GenericRow.of(1, 1)));
            commit.commit(1, writer.prepareCommit(true, 1));

            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .commitChanges(SchemaChange.addColumn("smallint", DataTypes.SMALLINT()));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            true,
                            (byte) 1,
                            1,
                            1L,
                            1.0f,
                            1.0d,
                            BinaryString.fromString("char1"),
                            BinaryString.fromString("varchar1"),
                            0,
                            Timestamp.fromMicros(1694505288000000L),
                            Timestamp.fromMicros(1694505288001000L),
                            Timestamp.fromMicros(1694505288001001L),
                            Decimal.fromUnscaledLong(10000, 10, 5),
                            new byte[] {0x01, 0x02, 0x03},
                            new GenericArray(new int[] {1, 1, 1}),
                            new GenericMap(Map.of(1, 1)),
                            GenericRow.of(1, 1),
                            (short) 1));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t101");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "a", DataTypes.STRING()),
                                    new DataField(1, "b", DataTypes.INT()),
                                    new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath6)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    List.of("a"),
                                    new HashMap<>() {
                                        {
                                            put(CoreOptions.BUCKET.key(), "1");
                                            put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
                                        }
                                    },
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath6);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            for (int i = 0; i < 10; i++) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(0, writer.prepareCommit(true, 0));

            writer.write(GenericRow.ofKind(RowKind.DELETE, BinaryString.fromString("a0"), 0, 0));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/t102");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "a", DataTypes.STRING()),
                                    new DataField(1, "b", DataTypes.INT()),
                                    new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>() {
                                        {
                                            put("file-index.bloom-filter.columns", "a,b,c");
                                        }
                                    },
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            for (int i = 0; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(0, writer.prepareCommit(true, 0));

            for (int i = 1; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(1, writer.prepareCommit(true, 1));

            for (int i = 2; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(2, writer.prepareCommit(true, 2));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/t103");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "id", DataTypes.INT()),
                                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>() {
                                        {
                                            put("file.format", "orc");
                                            put("primary-key", "id");
                                            put("bucket", "2");
                                        }
                                    },
                                    ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/t104");
            RowType rowType =
                    RowType.of(
                            new DataField(0, "server_recv_time", DataTypes.STRING(), "数据入库的时间"),
                            new DataField(1, "guid", DataTypes.STRING(), "用户id"),
                            new DataField(2, "mcc", DataTypes.STRING(), "移动国家码"),
                            new DataField(3, "mnc", DataTypes.STRING(), "移动网络码"),
                            new DataField(4, "andr_id", DataTypes.STRING(), "Android"),
                            new DataField(5, "devc_brand", DataTypes.STRING(), "设备品牌"),
                            new DataField(6, "ori_evnt_name", DataTypes.STRING(), "原始事件名"),
                            new DataField(7, "evnt_time", DataTypes.STRING(), "终端和服务端校正的时间"),
                            new DataField(8, "gg_ad_id", DataTypes.STRING(), "谷歌的 gaid"),
                            new DataField(9, "qua", DataTypes.STRING(), "qua 内容"),
                            new DataField(10, "qv", DataTypes.STRING(), "qua版本"),
                            new DataField(11, "pltfm", DataTypes.STRING(), "设备平台（Andr,IOS)"),
                            new DataField(12, "prdckt", DataTypes.STRING(), "产品名"),
                            new DataField(13, "pkg_name", DataTypes.STRING(), "包名"),
                            new DataField(14, "app_vern", DataTypes.STRING(), "app版本号"),
                            new DataField(15, "co", DataTypes.STRING(), "浏览器内核版本"),
                            new DataField(16, "chid", DataTypes.STRING(), "当前渠道号"),
                            new DataField(17, "acid", DataTypes.STRING(), "激活渠道号"),
                            new DataField(18, "mbl_mdl", DataTypes.STRING(), "机型代号"),
                            new DataField(19, "dn", DataTypes.STRING(), "机型名"),
                            new DataField(20, "rslv_rto", DataTypes.STRING(), "分辨率"),
                            new DataField(21, "os_vern", DataTypes.STRING(), "操作系统版本"),
                            new DataField(22, "api", DataTypes.STRING(), "操作系统api版本"),
                            new DataField(23, "cntry", DataTypes.STRING(), "国家"),
                            new DataField(24, "lng", DataTypes.STRING(), "语言"),
                            new DataField(25, "biz_value", DataTypes.STRING(), "业务上传内容值"),
                            new DataField(
                                    26,
                                    "biz_value_map",
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                    "业务上传内容值map"),
                            new DataField(
                                    27,
                                    "qua_map",
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                    "qua内容值map"),
                            new DataField(28, "ori_lng", DataTypes.STRING(), "原始的语言"),
                            new DataField(29, "ori_app_vern", DataTypes.STRING(), "原始的app版本"),
                            new DataField(30, "evnt_app_vern", DataTypes.STRING(), "事件上报时的app 版本"),
                            new DataField(
                                    31, "ori_evnt_app_vern", DataTypes.STRING(), "原始事件上报的app版本"),
                            new DataField(
                                    32,
                                    "network_type",
                                    DataTypes.STRING(),
                                    "0:无网,1:wifi,2:2G,3:3G,4:4G"),
                            new DataField(33, "network_sub_type", DataTypes.STRING(), "网络子类型"),
                            new DataField(
                                    34, "launch_chnnl_id", DataTypes.STRING(), "启动时的channel id"),
                            new DataField(35, "launch_posid", DataTypes.STRING(), "启动时的 posid"),
                            new DataField(36, "action_name", DataTypes.STRING(), "动作名称"),
                            new DataField(
                                    37,
                                    "biz_value_extra",
                                    DataTypes.STRING(),
                                    "业务扩展数据(不同的业务打点为json,或者&=分割或者单一字段)"),
                            new DataField(38, "login_time", DataTypes.STRING(), "启动时间"),
                            new DataField(39, "session_id", DataTypes.STRING(), "会话sessionid"),
                            new DataField(40, "un_region_en", DataTypes.STRING(), "国家所在大洲（联合国）"),
                            new DataField(
                                    41, "sub_un_region_en", DataTypes.STRING(), "国家所在大洲子区域（联合国）"),
                            new DataField(
                                    42,
                                    "unicef_region_en",
                                    DataTypes.STRING(),
                                    "国家所在文化区域（联合国儿童基金会）"),
                            new DataField(
                                    43,
                                    "sub_unicef_region_en",
                                    DataTypes.STRING(),
                                    "国家所在文化子区域（联合国儿童基金会）"),
                            new DataField(
                                    44,
                                    "is_lng_enable",
                                    DataTypes.STRING(),
                                    "是否语言可纳入分析计算 1 是，0否，UNKNOWN 未知"),
                            new DataField(
                                    45,
                                    "is_cntry_enable",
                                    DataTypes.STRING(),
                                    "是否国家可纳入分析计算 1 是，0否，UNKNOWN 未知"),
                            new DataField(46, "server_ts", DataTypes.STRING(), "数据入库的时间戳"),
                            new DataField(47, "event_ts", DataTypes.STRING(), "终端和服务端校正的时间戳"),
                            new DataField(48, "client_event_ts", DataTypes.STRING(), "终端手机时间戳"),
                            new DataField(49, "ds", DataTypes.BIGINT(), "分区字段"));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Arrays.asList("ds"),
                                    Collections.emptyList(),
                                    new HashMap<>(),
                                    ""));

            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            GenericRow record =
                    GenericRow.of(
                            BinaryString.fromString("2024-01-15 10:30:00"), // server_recv_time
                            BinaryString.fromString("8fab6f3f5f31e8e13b2dc841377988cb"), // guid
                            BinaryString.fromString("460"), // mcc
                            BinaryString.fromString("00"), // mnc
                            BinaryString.fromString("android_device_001"), // andr_id
                            BinaryString.fromString("Samsung"), // devc_brand
                            BinaryString.fromString("page_view"), // ori_evnt_name
                            BinaryString.fromString("2024-01-15 10:29:45"), // evnt_time
                            BinaryString.fromString("gaid_123456789"), // gg_ad_id
                            BinaryString.fromString(
                                    "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36"), // qua
                            BinaryString.fromString("1.0"), // qv
                            BinaryString.fromString("Andr"), // pltfm
                            BinaryString.fromString("MyApp"), // prdckt
                            BinaryString.fromString("com.example.myapp"), // pkg_name
                            BinaryString.fromString("19.5"), // app_vern
                            BinaryString.fromString("Chrome/91.0.4472.120"), // co
                            BinaryString.fromString("channel_001"), // chid
                            BinaryString.fromString("channel_001"), // acid
                            BinaryString.fromString("SM-G991B"), // mbl_mdl
                            BinaryString.fromString("Galaxy S21"), // dn
                            BinaryString.fromString("1080x2400"), // rslv_rto
                            BinaryString.fromString("Android 12"), // os_vern
                            BinaryString.fromString("31"), // api
                            BinaryString.fromString("CN"), // cntry
                            BinaryString.fromString("zh-CN"), // lng
                            BinaryString.fromString(
                                    "{\"page\":\"home\",\"action\":\"click\"}"), // biz_value
                            new GenericMap(
                                    Map.of(
                                            BinaryString.fromString("page"),
                                                    BinaryString.fromString("home"),
                                            BinaryString.fromString("action"),
                                                    BinaryString.fromString("click"),
                                            BinaryString.fromString("category"),
                                                    BinaryString.fromString(
                                                            "navigation"))), // biz_value_map
                            new GenericMap(
                                    Map.of(
                                            BinaryString.fromString("browser"),
                                                    BinaryString.fromString("Chrome"),
                                            BinaryString.fromString("version"),
                                                    BinaryString.fromString("91"),
                                            BinaryString.fromString("engine"),
                                                    BinaryString.fromString("WebKit"))), // qua_map
                            BinaryString.fromString("zh"), // ori_lng
                            BinaryString.fromString("1.2.0"), // ori_app_vern
                            BinaryString.fromString("1.2.3"), // evnt_app_vern
                            BinaryString.fromString("1.2.3"), // ori_evnt_app_vern
                            BinaryString.fromString("1"), // network_type (wifi)
                            BinaryString.fromString("WIFI"), // network_sub_type
                            BinaryString.fromString("channel_001"), // launch_chnnl_id
                            BinaryString.fromString("pos_001"), // launch_posid
                            BinaryString.fromString("click"), // action_name
                            BinaryString.fromString(
                                    "{\"extra\":\"data\",\"source\":\"button\"}"), // biz_value_extra
                            BinaryString.fromString("2024-01-15 10:29:00"), // login_time
                            BinaryString.fromString("session_123456"), // session_id
                            BinaryString.fromString("Asia"), // un_region_en
                            BinaryString.fromString("Eastern Asia"), // sub_un_region_en
                            BinaryString.fromString("East Asia and Pacific"), // unicef_region_en
                            BinaryString.fromString("China"), // sub_unicef_region_en
                            BinaryString.fromString("1"), // is_lng_enable
                            BinaryString.fromString("1"), // is_cntry_enable
                            BinaryString.fromString("1705302600000"), // server_ts
                            BinaryString.fromString("1705302585000"), // event_ts
                            BinaryString.fromString("1705302580000"), // client_event_ts
                            20251002L // ds (partition field - BIGINT)
                            );

            writer.write(record);
            commit.commit(writer.prepareCommit());
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner =
                    DistributedQueryRunner.builder(
                                    testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                            .build();
            queryRunner.installPlugin(new TrinoPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testsss() {
        //        String s = sql("SELECT\n" +
        //                "    ds\n" +
        //                "  , guid\n" +
        //                "  , ma_channel_id\n" +
        //                "  , ma_pos_id\n" +
        //                "  , is_ma_first_launch\n" +
        //                "    FROM paimon.default.t104\n" +
        //                "    WHERE ds = 20250930\n" +
        //                "      AND ma_prdckt = 'football'\n" +
        //                "      AND CAST(app_version AS DOUBLE) >= 19.5 limit 10");
        String s =
                sql(
                        "SELECT\n"
                                + "evnt_time\n"
                                + ", action_name , element_at(biz_value_map, 'resourceId')\n"
                                + "AS resourceld\n"
                                + ", element_at(biz_value_map, 'pre_loading_time') AS pre_loading_time , element_at(biz_value_map, 'item_position')\n"
                                + "AS item_position\n"
                                + ", element_at(biz_value_map, 'actual_progress')\n"
                                + "AS actual_progress\n"
                                + "FROM paimon.default.t104\n"
                                + "WHERE ds = 20251002\n"
                                + "AND app_vern = '19.5'\n"
                                + "AND element_at(biz_value_map, 'resourceType') = '5'\n"
                                + "AND guid = '8fab6f3f5f31e8e13b2dc841377988cb'\n"
                                + "ORDER BY evnt_time");
        System.out.println(s);
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new BigIntType()),
                                // test field name has upper case
                                new DataField(2, "aCa", new VarCharType()),
                                new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    @Test
    public void testComplexTypes() {
        assertThat(sql("SELECT * FROM paimon.default.t4"))
                .isEqualTo("[[1, {1=2}, [2, male], [1, 2, 3]]]");
    }

    @Test
    public void testEmptyTable() {
        assertThat(sql("SELECT * FROM paimon.default.empty_t")).isEqualTo("[]");
    }

    @Test
    public void testProjection() {
        assertThat(sql("SELECT * FROM paimon.default.t1"))
                .isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM paimon.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM paimon.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    public void testLimit() {
        assertThat(sql("SELECT * FROM paimon.default.t1 LIMIT 1")).isEqualTo("[[1, 2, 1, 1]]");
        assertThat(sql("SELECT * FROM paimon.default.t1 WHERE a = 5 LIMIT 1"))
                .isEqualTo("[[5, 6, 3, 3]]");
    }

    @Test
    public void testSystemTable() {
        assertThat(
                        sql(
                                "SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\""))
                .isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    public void testFilter() {
        assertThat(sql("SELECT a, aCa FROM paimon.default.t2 WHERE a < 4"))
                .isEqualTo("[[1, 1], [3, 2]]");
    }

    @Test
    public void testGroupByWithCast() {
        assertThat(
                        sql(
                                "SELECT pt, a, SUM(b), SUM(d) FROM paimon.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    @Test
    public void testLimitWithPartition() {
        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' LIMIT 1"))
                .isEqualTo("[[1, 1, 1, 1, 1]]");

        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' AND b = 2 LIMIT 1"))
                .isEqualTo("[[1, 1, 2, 2, 2]]");
    }

    @Test
    public void testShowCreateTable() {
        assertThat(sql("SHOW CREATE TABLE paimon.default.t3"))
                .isEqualTo(
                        "[[CREATE TABLE paimon.default.t3 (\n"
                                + "   pt varchar,\n"
                                + "   a integer,\n"
                                + "   b bigint,\n"
                                + "   c bigint,\n"
                                + "   d integer\n"
                                + ")]]");
    }

    @Test
    public void testCreateSchema() {
        sql("CREATE SCHEMA paimon.test");
        assertThat(sql("SHOW SCHEMAS FROM paimon"))
                .isEqualTo("[[default], [information_schema], [test]]");
        sql("DROP SCHEMA paimon.test");
    }

    @Test
    public void testDropSchema() {
        sql("CREATE SCHEMA paimon.tpch");
        sql("DROP SCHEMA paimon.tpch");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema]]");
    }

    @Test
    public void testCreateTable() {
        sql(
                "CREATE TABLE orders ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        assertThat(sql("SHOW TABLES FROM paimon.default")).contains("orders");
        sql("DROP TABLE IF EXISTS paimon.default.orders");
    }

    @Test
    public void testRenameTable() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME TO t6");
        String result = sql("SHOW TABLES FROM paimon.default");
        assertThat(result).doesNotContain("t5").contains("t6");
        sql("DROP TABLE IF EXISTS paimon.default.t6");
    }

    @Test
    public void testDropTable() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
        assertThat(sql("SHOW TABLES FROM paimon.default")).doesNotContain("t5");
    }

    @Test
    public void testAddColumn() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 ADD COLUMN zip varchar");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [order_status, varchar(2147483646), , ], [total_price, double, , ], [order_date, date, , ], [zip, varchar(2147483646), , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testRenameColumn() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME COLUMN order_status to g");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [g, varchar(2147483646), , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testDropColumn() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 DROP COLUMN order_status");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testSetTableProperties() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql(
                "ALTER TABLE paimon.default.t5 SET PROPERTIES bucket = '4',snapshot_time_retained = '4h'");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testAllType() {
        assertThat(
                        sql(
                                "SELECT boolean, tinyint, smallint,int,bigint,float,double,char,varchar, date,timestamp_0, "
                                        + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t99"))
                .isEqualTo(
                        "[[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, "
                                + "2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, "
                                + "0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    public void testTimeTravel() {
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 2"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");

        assertThat(
                        sql(
                                "SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                                        + timestampLiteral(t2FirstCommitTimestamp, 6)))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(
                        sql(
                                "SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                                        + timestampLiteral(System.currentTimeMillis(), 6)))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testTimeTravelWithTag() {
        // tag or snapshotId is string
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF '1'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 'tag-2'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
        // tag or snapshotId is int
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testSchemaEvolution() {
        assertThat(
                        sql(
                                "SELECT boolean, tinyint, smallint, int, bigint,float,double,char,varchar, date,timestamp_0, "
                                        + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t100"))
                .isEqualTo(
                        "[[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    public void testDeletionFile() {
        assertThat(sql("SELECT * FROM paimon.default.t101"))
                .isEqualTo(
                        "[[a1, 1, 1], [a2, 2, 2], [a3, 3, 3], [a4, 4, 4], [a5, 5, 5], [a6, 6, 6], [a7, 7, 7], [a8, 8, 8], [a9, 9, 9]]");
    }

    @Test
    public void testFileIndex() {
        assertThat(sql("SELECT * FROM paimon.default.t102 where c = 2")).isEqualTo("[[a2, 2, 2]]");
    }

    @Test
    public void testInsertInto() {
        sql(
                "INSERT INTO paimon.default.t103 VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(5,'5'),(6,'6')");
        assertThat(sql("SELECT * FROM paimon.default.t103 order by id asc"))
                .isEqualTo("[[1, 1], [2, 2], [3, 3], [4, 4], [5, 5], [6, 6]]");
    }

    protected String sql(String sql) {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }

    protected static String timestampLiteral(long epochMilliSeconds, int precision) {
        return DateTimeFormatter.ofPattern(
                        "''yyyy-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
