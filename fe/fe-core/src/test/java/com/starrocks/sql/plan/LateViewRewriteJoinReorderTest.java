// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LateViewRewriteJoinReorderTest extends PlanTestNoneDBBase {
    private static final String DATABASE = "late_view_join_reorder_repro";
    private static final String QUERY = "SELECT id FROM v UNION ALL "
            + "SELECT v.id FROM v JOIN c ON v.id = c.id WHERE v.source = 1";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testEarlyViewRewriteCanJoinSelectiveTableFirst() throws Exception {
        assertSelectiveTableJoinedFirst(getPlan(true));
    }

    @Test
    public void testLateViewRewriteCanJoinSelectiveTableFirst() throws Exception {
        assertSelectiveTableJoinedFirst(getPlan(false));
    }

    private ExecPlan getPlan(boolean multiStages) throws Exception {
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        var state = connectContext.getGlobalStateMgr();
        var previousStatistics = state.getStatisticStorage();
        SessionVariable session = (SessionVariable) previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableMaterializedViewMultiStagesRewrite(multiStages);
        session.setOptimizerExecuteTimeout(120000);
        session.setOptimizerMaterializedViewTimeLimitMillis(120000);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
            starRocksAssert.withTable("CREATE TABLE t (id INT, k INT, source INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withTable("CREATE TABLE b (k INT) DUPLICATE KEY(k) "
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withTable("CREATE TABLE c (id INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            setTableStatistics((OlapTable) starRocksAssert.getTable(DATABASE, "t"), 1_000_000);
            setTableStatistics((OlapTable) starRocksAssert.getTable(DATABASE, "b"), 1_000_000);
            setTableStatistics((OlapTable) starRocksAssert.getTable(DATABASE, "c"), 1);
            state.setStatisticStorage(new EmptyStatisticStorage() {
                @Override
                public ColumnStatistic getColumnStatistic(Table table, String column) {
                    if (table.getName().equals("t") && column.equals("id")) {
                        return statistic(1, 1_000_000, 1_000_000);
                    }
                    if ((table.getName().equals("t") || table.getName().equals("b")) && column.equals("k")) {
                        return statistic(1, 100, 100);
                    }
                    if (table.getName().equals("t") && column.equals("source")) {
                        return statistic(0, 1, 2);
                    }
                    if (table.getName().equals("c") && column.equals("id")) {
                        return statistic(1, 1, 1);
                    }
                    return ColumnStatistic.unknown();
                }
            });
            starRocksAssert.withView("CREATE VIEW v AS "
                    + "SELECT t.id, t.k, t.source FROM t JOIN b ON t.k = b.k");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') "
                    + "AS SELECT id FROM v");

            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, QUERY).second;
            PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL), "TABLE: mv", "UNION");
            Assertions.assertEquals(1, plan.getOutputExprs().size());
            return plan;
        } finally {
            try {
                starRocksAssert.dropDatabase(DATABASE);
            } finally {
                state.setStatisticStorage(previousStatistics);
                connectContext.setDatabase(previousDatabase);
                connectContext.setSessionVariable(previousSession);
            }
        }
    }

    private static ColumnStatistic statistic(double min, double max, double distinct) {
        return ColumnStatistic.builder()
                .setMinValue(min)
                .setMaxValue(max)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(distinct)
                .build();
    }

    private void assertSelectiveTableJoinedFirst(ExecPlan plan) {
        var nodes = new ArrayList<>(List.of(plan.getPhysicalPlan()));
        for (int i = 0; i < nodes.size(); i++) {
            nodes.addAll(nodes.get(i).getInputs());
        }
        List<Set<String>> firstJoins = nodes.stream()
                .filter(node -> node.getOp() instanceof PhysicalJoinOperator)
                .map(this::scanTables)
                .filter(tables -> tables.size() == 2)
                .toList();
        Assertions.assertEquals(List.of(Set.of("t", "c")), firstJoins,
                () -> "The selective c must join t before the broad t-b join. First joins: " + firstJoins
                        + "\n" + plan.getExplainString(TExplainLevel.COSTS));
    }

    private Set<String> scanTables(OptExpression root) {
        var nodes = new ArrayList<>(List.of(root));
        for (int i = 0; i < nodes.size(); i++) {
            nodes.addAll(nodes.get(i).getInputs());
        }
        return nodes.stream()
                .filter(node -> node.getOp() instanceof PhysicalOlapScanOperator)
                .map(node -> ((PhysicalOlapScanOperator) node.getOp()).getTable().getName())
                .collect(Collectors.toSet());
    }
}
