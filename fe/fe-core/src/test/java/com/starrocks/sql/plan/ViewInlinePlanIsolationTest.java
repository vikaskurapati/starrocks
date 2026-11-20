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

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.SeparateProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ViewInlinePlanIsolationTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testNormalizingOneInlinePlanDoesNotChangeLaterReuse() throws Exception {
        String database = "view_inline_plan_isolation";
        String previousDatabase = connectContext.getDatabase();
        var previousSession = connectContext.getSessionVariable();
        var session = previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(database).useDatabase(database);
            starRocksAssert.withTable("CREATE TABLE t (id INT, source INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS SELECT id, CAST(source AS BIGINT) source64 FROM t");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') AS SELECT id FROM v");
            ColumnRefFactory factory = new ColumnRefFactory();
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "SELECT * FROM v", connectContext);
            var logical = new RelationTransformer(factory, connectContext).transform(statement.getQueryRelation());
            List<OptExpression> nodes = new ArrayList<>(List.of(logical.getRoot()));
            for (int i = 0; i < nodes.size(); i++) {
                nodes.addAll(nodes.get(i).getInputs());
            }
            OptExpression inline = nodes.stream()
                    .filter(node -> node.getOp().getEquivalentOp() instanceof LogicalViewScanOperator)
                    .findFirst().orElseThrow();
            LogicalViewScanOperator view = (LogicalViewScanOperator) inline.getOp().getEquivalentOp();
            view.setOriginalPlanEvaluator(new LogicalViewScanOperator.OptPlanEvaluator(
                    MvUtils.cloneExpression(inline), connectContext, view.getOutputColumnSet(), factory));
            LogicalViewScanOperator bareView = LogicalViewScanOperator.builder().withOperator(view)
                    .setProjection(null).setPredicate(null).build();

            OptExpression first = MvUtils.replaceLogicalViewScanOperator(OptExpression.create(bareView));
            Assertions.assertNotNull(first.getOp().getProjection(),
                    "The inline plan needs a projection to compute CAST(source AS BIGINT)");
            var expected = Map.copyOf(first.getOp().getProjection().getColumnRefMap());
            var context = OptimizerFactory.initContext(connectContext, factory);
            var task = new TaskContext(context, new PhysicalPropertySet(), view.getOutputColumnSet(), Double.MAX_VALUE);
            // This existing normalization rule edits the operator in place. Its edits belong to this
            // query candidate and must not alter the evaluator's cached plan for a subsequent candidate.
            new SeparateProjectRule().rewrite(OptExpression.create(new LogicalTreeAnchorOperator(), first), task);
            Assertions.assertNull(first.getOp().getProjection());

            OptExpression second = MvUtils.replaceLogicalViewScanOperator(OptExpression.create(bareView));
            Assertions.assertNotNull(second.getOp().getProjection(),
                    "Reusing the view must retain its computed-column projection after another candidate is normalized");
            Assertions.assertEquals(expected, second.getOp().getProjection().getColumnRefMap());
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
