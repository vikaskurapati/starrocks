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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MaterializedViewOptimizer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensationBuilder;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyScanRule;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Uses an actual MV refresh and base-table update to verify the partial-refresh view guards,
 * plus partition coverage in the full planner. The pseudo backends supply refresh/version metadata;
 * these assertions validate plans rather than executing query results.
 */
public class PartialRefreshViewRewriteGuardTest extends MVTestBase {
    @Test
    public void testProjectedViewScanRejectsUnknownPartialRefresh() throws Exception {
        assertPartialRefreshGuard(false);
    }

    @Test
    public void testAggregateViewScanRejectsUnknownPartialRefresh() throws Exception {
        assertPartialRefreshGuard(true);
    }

    private void assertPartialRefreshGuard(boolean aggregate) throws Exception {
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableCBOViewBasedMvRewrite(false);
        session.setEnableForceRuleBasedMvRewrite(true);
        session.setEnableCboBasedMvRewrite(false);
        session.setEnableMaterializedViewUnionRewrite(true);
        session.setEnableMaterializedViewTextMatchRewrite(false);
        session.setMaterializedViewRewriteMode("default");
        session.setOptimizerExecuteTimeout(120000);
        session.setOptimizerMaterializedViewTimeLimitMillis(120000);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withTable("CREATE TABLE guard_t (id INT NOT NULL, source INT) DUPLICATE KEY(id) "
                    + "PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN ('10'), "
                    + "PARTITION p2 VALUES LESS THAN ('20')) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            cluster.runSql(DB_NAME, "INSERT INTO guard_t VALUES (1, 100), (11, 200)");
            starRocksAssert.withView("CREATE VIEW guard_v AS SELECT id, source FROM guard_t");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW guard_mv PARTITION BY id "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='checked') "
                    + "AS SELECT id, source FROM guard_v");
            refreshMaterializedView(DB_NAME, "guard_mv");

            MaterializedView mv = getMv("guard_mv");
            String sql = aggregate ? "SELECT id, COUNT(*) FROM guard_v GROUP BY id" : "SELECT id FROM guard_v";
            assertViewGuard(mv, sql, aggregate, false);

            // p1 remains fresh; inserting into p2 makes precisely that MV partition stale.
            cluster.runSql(DB_NAME, "INSERT INTO guard_t PARTITION(p2) VALUES (12, 300)");
            assertViewGuard(mv, sql, aggregate, true);

            for (boolean multiStages : List.of(true, false)) {
                session.setEnableMaterializedViewMultiStagesRewrite(multiStages);
                ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
                Assertions.assertEquals(aggregate ? 2 : 1, plan.getOutputExprs().size());
                assertEachPartitionCoveredOnce(plan, mv);
            }
        } finally {
            try {
                starRocksAssert.dropMaterializedView("guard_mv");
                starRocksAssert.dropView("guard_v");
                starRocksAssert.dropTable("guard_t");
            } finally {
                connectContext.setSessionVariable(previousSession);
            }
        }
    }

    private void assertViewGuard(MaterializedView mv, String sql, boolean aggregate, boolean partial) throws Exception {
        ColumnRefFactory factory = new ColumnRefFactory();
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        LogicalPlan logical = new RelationTransformer(new TransformerContext(factory, connectContext,
                new MVTransformerContext(connectContext, false))).transform(statement.getQueryRelation());
        OptExpression query = MvUtils.optimizeViewPlan(logical.getRoot(), connectContext,
                new ColumnRefSet(logical.getOutputColumn()), factory);
        List<LogicalViewScanOperator> views = MvUtils.getScanOperator(query).stream()
                .filter(scan -> scan instanceof LogicalViewScanOperator)
                .map(scan -> (LogicalViewScanOperator) scan)
                .toList();
        Assertions.assertEquals(1, views.size(), "Exercise a real non-inlined view scan");
        Assertions.assertNotNull(views.get(0).getProjection(), "Exercise the projected-view input shape");

        OptimizerContext context = OptimizerFactory.initContext(connectContext, factory);
        MvUpdateInfo update = MvRefreshArbiter.getMVTimelinessUpdateInfo(mv,
                MVTimelinessArbiter.QueryRewriteParams.ofQueryRewrite(context));
        Assertions.assertEquals(TableProperty.QueryRewriteConsistencyMode.CHECKED,
                update.getQueryRewriteConsistencyMode());
        if (partial) {
            Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.PARTIAL, update.getMVToRefreshType());
            Assertions.assertEquals(Set.of("p2"), update.getMVToRefreshPCells().getPartitionNames(),
                    "The test must have both a fresh partition and a stale partition");
        } else {
            Assertions.assertTrue(update.getMVToRefreshPCells().getPartitionNames().isEmpty(),
                    "The fully refreshed MV is the positive control");
        }

        MvPlanContext mvPlan = new MaterializedViewOptimizer().optimize(mv, connectContext);
        Assertions.assertTrue(mvPlan.isValidMvPlan(), mvPlan.getInvalidReason());
        MaterializationContext materialization = new MaterializationContext(context, mv, mvPlan.getLogicalPlan(),
                factory, mvPlan.getRefFactory(), MvUtils.getAllTables(mvPlan.getLogicalPlan()), List.of(),
                update, mvPlan.getOutputColumns(), 0);
        MVCompensation compensation = new MVCompensationBuilder(materialization, update)
                .buildMvCompensation(Optional.of(query));
        Assertions.assertEquals(partial ? MVTransparentState.UNKNOWN : MVTransparentState.NO_COMPENSATE,
                compensation.getState());
        Assertions.assertEquals(partial, materialization.isNoRewrite(query),
                "CHECKED mode must reject unknown view compensation, but allow a fully fresh MV");

        Rule rule = aggregate ? AggregateScanRule.getInstance() : OnlyScanRule.getInstance();
        if (partial) {
            Assertions.assertNull(MvPartitionCompensator.compensateQueryPartitionPredicate(
                    materialization, rule, factory, query),
                    "Unsupported view-scan partition compensation must stop before query-branch construction");
        } else {
            Assertions.assertNotNull(MvPartitionCompensator.compensateQueryPartitionPredicate(
                    materialization, rule, factory, query));
        }
    }

    private void assertEachPartitionCoveredOnce(ExecPlan plan, MaterializedView mv) {
        Map<String, Integer> coverage = new HashMap<>();
        List<OptExpression> nodes = new ArrayList<>(List.of(plan.getPhysicalPlan()));
        for (int i = 0; i < nodes.size(); i++) {
            nodes.addAll(nodes.get(i).getInputs());
        }
        List<PhysicalOlapScanOperator> scans = nodes.stream()
                .filter(node -> node.getOp() instanceof PhysicalOlapScanOperator)
                .map(node -> (PhysicalOlapScanOperator) node.getOp())
                .toList();
        Assertions.assertFalse(scans.isEmpty());
        for (PhysicalOlapScanOperator scan : scans) {
            Assertions.assertTrue(Set.of("guard_t", "guard_mv").contains(scan.getTable().getName()));
            Set<String> partitions = scan.getSelectedPartitionId().stream()
                    .map(id -> ((OlapTable) scan.getTable()).getPartition(id).getName())
                    .collect(Collectors.toSet());
            if (scan.getTable().getName().equals(mv.getName())) {
                Assertions.assertFalse(partitions.contains("p2"), "Never read stale MV partition p2");
            }
            partitions.forEach(partition -> coverage.merge(partition, 1, Integer::sum));
        }
        Assertions.assertEquals(Map.of("p1", 1, "p2", 1), coverage,
                () -> "MV and base branches must cover each partition exactly once.\n"
                        + plan.getExplainString(TExplainLevel.NORMAL));
    }
}
