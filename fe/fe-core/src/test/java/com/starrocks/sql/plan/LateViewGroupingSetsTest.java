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

import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.RepeatNode;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.rule.transformation.SeparateProjectRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class LateViewGroupingSetsTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testLateViewPruningPreservesGroupingSetsCteSchema() throws Exception {
        assertGroupingSetsCteSchema(true);
    }

    @Test
    public void testGroupingSetsCteSchemaWithoutViewRewrite() throws Exception {
        assertGroupingSetsCteSchema(false);
    }

    private void assertGroupingSetsCteSchema(boolean rewriteView) throws Exception {
        String database = "late_view_grouping_sets_repro";
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = previousSession.clone();
        session.setEnableMaterializedViewRewrite(rewriteView);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableMaterializedViewMultiStagesRewrite(false);
        session.setCboCTERuseRatio(0);
        session.setOptimizerExecuteTimeout(120000);
        session.setOptimizerMaterializedViewTimeLimitMillis(120000);
        connectContext.setSessionVariable(session);
        AtomicBoolean prunedGroupingSets = new AtomicBoolean();
        new MockUp<SeparateProjectRule>() {
            @Mock
            public OptExpression rewrite(Invocation invocation, OptExpression tree, TaskContext taskContext) {
                List<OptExpression> nodes = new ArrayList<>(List.of(tree));
                for (int i = 0; i < nodes.size(); i++) {
                    nodes.addAll(nodes.get(i).getInputs());
                }
                if (Arrays.stream(Thread.currentThread().getStackTrace())
                        .anyMatch(frame -> frame.getMethodName().equals("pruneInlinedViewPlan"))
                        && nodes.stream().anyMatch(node -> node.getOp() instanceof LogicalRepeatOperator
                        && node.getOp().getProjection() != null)) {
                    prunedGroupingSets.set(true);
                }
                return invocation.proceed(tree, taskContext);
            }
        };
        try {
            starRocksAssert.withDatabase(database).useDatabase(database);
            starRocksAssert.withTable("CREATE TABLE t (id INT, k INT, source INT, amount INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withTable("CREATE TABLE c (id INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS SELECT id, k, source, amount FROM t");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') AS SELECT id FROM v");
            String sql = "WITH gs AS (SELECT k, amount, avg(DISTINCT amount) av, sum(DISTINCT amount) sm, "
                    + "count(DISTINCT amount, k) cnt FROM t GROUP BY ROLLUP(k, amount)) "
                    + "SELECT id, NULL av, NULL sm, NULL cnt FROM v UNION ALL "
                    + "SELECT x.id, a.av + b.av, a.sm + b.sm, a.cnt + b.cnt "
                    + "FROM v x JOIN c ON x.id = c.id CROSS JOIN gs a CROSS JOIN gs b WHERE x.source = 1";
            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            String explain = plan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(explain, "REPEAT_NODE", "MultiCastDataSinks");
            if (rewriteView) {
                PlanTestBase.assertContains(explain, "TABLE: mv");
            }
            Assertions.assertEquals(rewriteView, prunedGroupingSets.get(),
                    "Only the MV rewrite must exercise an already projected Repeat during late pruning");
            var producers = plan.getFragments().stream()
                    .filter(fragment -> fragment instanceof MultiCastPlanFragment)
                    .map(fragment -> (MultiCastPlanFragment) fragment)
                    .filter(fragment -> fragment.getPlanRoot() instanceof RepeatNode
                            || (fragment.getPlanRoot() instanceof ProjectNode
                            && fragment.getPlanRoot().getChild(0) instanceof RepeatNode))
                    .toList();
            Assertions.assertFalse(producers.isEmpty(), explain);
            for (var producer : producers) {
                Assertions.assertEquals(1, producer.getPlanRoot().getTupleIds().size(),
                        "The grouping-sets multicast producer must expose a single materialized output tuple\n" + explain);
                ProjectNode project = Assertions.assertInstanceOf(ProjectNode.class, producer.getPlanRoot(),
                        "Preserve the materializing projection required by the grouping-sets CTE");
                var outputSlots = project.getSlotMap().keySet().stream().map(slot -> slot.asInt())
                        .collect(Collectors.toSet());
                for (var destination : producer.getDestNodeList()) {
                    Assertions.assertTrue(outputSlots.containsAll(destination.getReceiveColumns()),
                            "Every consumed CTE column must be present in the producer's output\n" + explain);
                }
            }
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
