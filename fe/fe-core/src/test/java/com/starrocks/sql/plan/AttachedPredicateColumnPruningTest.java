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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL regressions for view predicates across UNION, WINDOW, and VALUES roots.
 * Planning must succeed and the source filter must remain in the executable plan.
 */
public class AttachedPredicateColumnPruningTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testUnionPredicateColumnPruning() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("union_predicate_repro",
                "SELECT id, source FROM t UNION ALL SELECT id, source FROM t",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
        assertUnionOutputsOnlyId(plan);
    }

    @Test
    public void testUnionPredicateColumnPruningWithoutCte() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("union_predicate_without_cte",
                "SELECT id, source FROM t UNION ALL SELECT id, source FROM t",
                "SELECT id FROM v UNION ALL SELECT id FROM v WHERE source = 1");
        assertUnionOutputsOnlyId(plan);
    }

    @Test
    public void testUnionPredicateColumnPruningWithLateMvRewrite() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("union_predicate_late_rewrite",
                "SELECT id, source FROM t UNION ALL SELECT id, source FROM t",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b", "source", false);
        assertUnionOutputsOnlyId(plan);
    }

    @Test
    public void testUnusedWindowOverUnionPrunesPredicateColumn() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("unused_window_over_union",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn "
                        + "FROM (SELECT id, source FROM t UNION ALL SELECT id, source FROM t) u",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
        assertUnionOutputsOnlyId(plan);
        Assertions.assertFalse(getPlanNodes(plan.getPhysicalPlan()).stream()
                .anyMatch(node -> node.getOp() instanceof PhysicalWindowOperator));
    }

    @Test
    public void testSelectedPredicateColumnIsPreserved() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("selected_predicate_column",
                "SELECT id, source FROM t UNION ALL SELECT id, source FROM t",
                "WITH c AS (SELECT source FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.source FROM c a CROSS JOIN c b");
        Assertions.assertTrue(getPlanNodes(plan.getPhysicalPlan()).stream()
                .anyMatch(node -> node.getOp() instanceof PhysicalUnionOperator union
                        && union.getOutputColumnRefOp().stream().anyMatch(column -> column.getName().equals("source"))),
                "A predicate column selected by the caller must remain in the UNION outputs");
    }

    @Test
    public void testWindowPredicateColumnPruning() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("window_predicate_repro",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id, rn FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.rn FROM c a CROSS JOIN c b");
        assertSourceFilterAboveWindow(plan);
    }

    @Test
    public void testWindowPredicateColumnPruningWithLateMvRewrite() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("window_predicate_late_rewrite",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id, rn FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.rn FROM c a CROSS JOIN c b", "source", false);
        assertSourceFilterAboveWindow(plan);
    }

    @Test
    public void testWindowPartitionPredicatePushdown() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("window_partition_predicate",
                "SELECT id, source, ROW_NUMBER() OVER (PARTITION BY source ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id, rn FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.rn FROM c a CROSS JOIN c b");
        Assertions.assertTrue(getPlanNodes(plan.getPhysicalPlan()).stream()
                        .anyMatch(node -> node.getOp() instanceof PhysicalOlapScanOperator scan
                                && scan.getTable().getName().equals("t")
                                && scan.getPredicate() != null && scan.getPredicate().toString().contains("source")),
                "A predicate on the window partition column can be evaluated at the scan");
    }

    @Test
    public void testWindowPredicateWhenWindowResultIsUnused() throws Exception {
        assertPredicateSurvivesPruning("unused_window_predicate_repro",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
    }

    @Test
    public void testWindowResultUsedOnlyByPredicate() throws Exception {
        assertPredicateSurvivesPruning("window_result_predicate_repro",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id FROM v WHERE rn = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b",
                "row_number");
    }

    @Test
    public void testViewPredicateOnProjectedExpression() throws Exception {
        assertPredicateSurvivesPruning("projected_predicate_repro",
                "SELECT id, CAST(source AS BIGINT) AS source_alias, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id, rn FROM v WHERE source_alias = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.rn FROM c a CROSS JOIN c b");
    }

    @Test
    public void testValuesPredicateColumnPruning() throws Exception {
        assertPredicateSurvivesPruning("values_predicate_repro",
                "SELECT id, source FROM t WHERE FALSE UNION ALL SELECT 1, 2 UNION ALL SELECT 2, 1",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
    }

    @Test
    public void testViewPredicatePreservesOriginalViewFilter() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("original_filter_predicate_repro",
                "SELECT id, source FROM t WHERE id > 0",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
        List<String> predicates = plan.getFragments().stream()
                .flatMap(fragment -> fragment.collectNodes().stream())
                .flatMap(node -> node.getConjuncts().stream())
                .map(ExprToSql::toSql)
                .toList();
        Assertions.assertTrue(predicates.stream().anyMatch(predicate -> predicate.contains("id")
                        && (predicate.contains("> 0") || predicate.contains(">= 1"))),
                () -> "The view's original filter was lost: " + predicates);
    }

    private void assertUnionOutputsOnlyId(ExecPlan plan) {
        List<PhysicalUnionOperator> unions = getPlanNodes(plan.getPhysicalPlan()).stream()
                .filter(node -> node.getOp() instanceof PhysicalUnionOperator)
                .map(node -> (PhysicalUnionOperator) node.getOp())
                .toList();
        Assertions.assertFalse(unions.isEmpty(), "The reproduction must retain a UNION");
        for (PhysicalUnionOperator union : unions) {
            Assertions.assertNull(union.getPredicate());
            Assertions.assertEquals(List.of("id"), union.getOutputColumnRefOp().stream()
                    .map(column -> column.getName()).toList());
            for (var childOutput : union.getChildOutputColumns()) {
                Assertions.assertEquals(union.getOutputColumnRefOp().size(), childOutput.size(),
                        "Pruning must keep UNION outputs and branch mappings aligned");
            }
        }
    }

    private void assertSourceFilterAboveWindow(ExecPlan plan) {
        Assertions.assertTrue(getPlanNodes(plan.getPhysicalPlan()).stream()
                        .anyMatch(node -> node.getOp() instanceof PhysicalFilterOperator filter
                                && filter.getPredicate().toString().contains("source")
                                && getPlanNodes(node).stream()
                                .anyMatch(child -> child.getOp() instanceof PhysicalWindowOperator)),
                "Filtering source must happen after ROW_NUMBER is evaluated");
        Assertions.assertTrue(getPlanNodes(plan.getPhysicalPlan()).stream()
                        .filter(node -> node.getOp() instanceof PhysicalWindowOperator)
                        .flatMap(node -> getPlanNodes(node).stream())
                        .filter(node -> node.getOp() instanceof PhysicalOlapScanOperator scan
                                && scan.getTable().getName().equals("t"))
                        .allMatch(node -> node.getOp().getPredicate() == null
                                || !node.getOp().getPredicate().toString().contains("source")),
                "Pushing source = 1 below the unpartitioned window would change row numbers");
    }

    private List<OptExpression> getPlanNodes(OptExpression root) {
        List<OptExpression> nodes = new ArrayList<>(List.of(root));
        for (int i = 0; i < nodes.size(); i++) {
            nodes.addAll(nodes.get(i).getInputs());
        }
        return nodes;
    }

    private ExecPlan assertPredicateSurvivesPruning(String database, String viewSql, String query) throws Exception {
        return assertPredicateSurvivesPruning(database, viewSql, query, "source");
    }

    private ExecPlan assertPredicateSurvivesPruning(String database, String viewSql, String query,
                                                    String predicateColumn) throws Exception {
        return assertPredicateSurvivesPruning(database, viewSql, query, predicateColumn, true);
    }

    private ExecPlan assertPredicateSurvivesPruning(String database, String viewSql, String query,
                                                    String predicateColumn, boolean multiStages) throws Exception {
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = (SessionVariable) previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableMaterializedViewMultiStagesRewrite(multiStages);
        session.setOptimizerExecuteTimeout(120000);
        session.setOptimizerMaterializedViewTimeLimitMillis(120000);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(database).useDatabase(database);
            starRocksAssert.withTable("CREATE TABLE t (id INT, source INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS " + viewSql);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') "
                    + "AS SELECT id FROM v");

            ExecPlan plan = Assertions.assertDoesNotThrow(
                    () -> UtFrameUtils.getPlanAndFragment(connectContext, query).second,
                    "A predicate-only column must survive until its filter is evaluated: " + query);
            PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL), "TABLE: mv");
            Assertions.assertEquals(1, plan.getOutputExprs().size(), "The query selects only one output column");
            List<String> predicates = plan.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .flatMap(node -> node.getConjuncts().stream())
                    .map(ExprToSql::toSql)
                    .toList();
            Assertions.assertTrue(predicates.stream()
                            .anyMatch(predicate -> predicate.contains(predicateColumn) && predicate.contains("= 1")),
                    () -> "The " + predicateColumn + " = 1 filter was lost: " + predicates + "\n"
                            + plan.getExplainString(TExplainLevel.NORMAL));
            return plan;
        } finally {
            try {
                starRocksAssert.dropDatabase(database);
            } finally {
                connectContext.setDatabase(previousDatabase);
                connectContext.setSessionVariable(previousSession);
            }
        }
    }
}
