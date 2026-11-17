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

import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class LateViewRewriteHivePredicateTest extends PlanTestNoneDBBase {
    private static final String DATABASE = "late_view_hive_predicate_repro";
    private static final String QUERY = "SELECT id FROM v UNION ALL "
            + "SELECT id FROM v WHERE source > 0";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testEarlyViewRewritePreservesExecutableHivePredicate() throws Exception {
        assertExecutableHivePredicate(true);
    }

    @Test
    public void testLateViewRewritePreservesExecutableHivePredicate() throws Exception {
        assertExecutableHivePredicate(false);
    }

    private void assertExecutableHivePredicate(boolean multiStages) throws Exception {
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = (SessionVariable) previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableCBOViewBasedMvRewrite(false);
        session.setEnableMaterializedViewMultiStagesRewrite(multiStages);
        // FORCE rewrite mode implicitly enables multi-stage rewriting even when its flag is false.
        session.setMaterializedViewRewriteMode("default");
        session.setEnableForceRuleBasedMvRewrite(true);
        session.setEnableCboBasedMvRewrite(false);
        session.setEnableMaterializedViewTextMatchRewrite(false);
        // The SQL UNION ALL combines two view usages; it is not MV partition compensation.
        // Disable compensation so its existing re-pruning path cannot mask the late-view bug.
        session.setEnableMaterializedViewUnionRewrite(false);
        session.setOptimizerExecuteTimeout(120000);
        session.setOptimizerMaterializedViewTimeLimitMillis(120000);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
            starRocksAssert.withView("CREATE VIEW v AS "
                    + "SELECT s.s_suppkey AS id, s.s_acctbal AS source "
                    + "FROM hive0.tpch.supplier s JOIN hive0.tpch.nation n "
                    + "ON s.s_nationkey = n.n_nationkey");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') "
                    + "AS SELECT id FROM v");

            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, QUERY).second;
            String explain = plan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(explain, "UNION", "TABLE: mv");
            Assertions.assertEquals(1, plan.getOutputExprs().size());
            Assertions.assertEquals(1, plan.getScanNodes().stream()
                    .filter(scan -> scan instanceof OlapScanNode olap
                            && olap.getOlapTable().getName().equals("mv"))
                    .count(), "Only the unfiltered view usage can use the id-only MV");

            List<OptExpression> nodes = new ArrayList<>(List.of(plan.getPhysicalPlan()));
            for (int i = 0; i < nodes.size(); i++) {
                nodes.addAll(nodes.get(i).getInputs());
            }
            List<PhysicalHiveScanOperator> supplierOperators = nodes.stream()
                    .filter(node -> node.getOp() instanceof PhysicalHiveScanOperator)
                    .map(node -> (PhysicalHiveScanOperator) node.getOp())
                    .filter(scan -> scan.getTable().getName().equals("supplier"))
                    .toList();
            Assertions.assertEquals(1, supplierOperators.size(),
                    "The filtered view usage must be expanded to the underlying Hive join");
            PhysicalHiveScanOperator supplier = supplierOperators.get(0);
            Assertions.assertTrue(Utils.extractConjuncts(supplier.getPredicate()).stream()
                            .anyMatch(predicate -> isBalancePredicate(predicate.toString())),
                    "The optimizer must push source > 0 into the supplier scan");

            List<HdfsScanNode> supplierScans = plan.getScanNodes().stream()
                    .filter(scan -> scan instanceof HdfsScanNode)
                    .map(scan -> (HdfsScanNode) scan)
                    .filter(scan -> scan.getTableName().equals("supplier"))
                    .toList();
            Assertions.assertEquals(1, supplierScans.size());
            List<String> executablePredicates = supplierScans.get(0).getScanNodePredicates()
                    .getNonPartitionConjuncts().stream()
                    .map(ExprToSql::toSql)
                    .toList();
            // supplier is unpartitioned: the predicate must be a row filter. These are the conjuncts
            // HdfsScanNode serializes for BE execution; scan.getPredicate() alone is insufficient.
            Assertions.assertTrue(executablePredicates.stream()
                            .anyMatch(LateViewRewriteHivePredicateTest::isBalancePredicate),
                    () -> "source > 0 exists on the optimizer scan but is missing from the executable Hive scan. "
                            + "ScanOperatorPredicates: " + supplier.getScanOperatorPredicates().getNonPartitionConjuncts()
                            + "; executable predicates: " + executablePredicates + "\n" + explain);
        } finally {
            try {
                starRocksAssert.dropDatabase(DATABASE);
            } finally {
                connectContext.setDatabase(previousDatabase);
                connectContext.setSessionVariable(previousSession);
            }
        }
    }

    private static boolean isBalancePredicate(String predicate) {
        return predicate.contains("s_acctbal") && predicate.contains("> 0");
    }
}
