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

import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyScanRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ViewOperatorCompensationTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testUnionRootCompensationRetainsUnselectedColumn() throws Exception {
        assertCompensationSurvives(false, false);
    }

    @Test
    public void testWindowRootCompensationRetainsUnselectedColumn() throws Exception {
        assertCompensationSurvives(true, false);
    }

    @Test
    public void testUnionJoinChildCompensationRetainsUnselectedColumn() throws Exception {
        assertCompensationSurvives(false, true);
    }

    @Test
    public void testWindowJoinChildCompensationRetainsUnselectedColumn() throws Exception {
        assertCompensationSurvives(true, true);
    }

    private void assertCompensationSurvives(boolean window, boolean join) throws Exception {
        String database = "view_operator_compensation_repro";
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = previousSession.clone();
        session.setEnableMaterializedViewRewrite(false);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(database).useDatabase(database);
            starRocksAssert.withTable("CREATE TABLE t (id INT NOT NULL, source INT NOT NULL) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS " + (window
                    ? "SELECT id, source, row_number() OVER (ORDER BY id) rn FROM t"
                    : "SELECT id, source FROM t UNION ALL SELECT id, source FROM t"));
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') AS SELECT id FROM t");
            ColumnRefFactory factory = new ColumnRefFactory();
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "SELECT * FROM v", connectContext);
            var logical = new RelationTransformer(factory, connectContext).transform(statement.getQueryRelation());
            var viewNode = nodes(logical.getRoot()).stream()
                    .filter(node -> window ? node.getOp() instanceof LogicalWindowOperator
                            : node.getOp() instanceof LogicalUnionOperator).findFirst().orElseThrow();
            OptExpression query = MvUtils.cloneExpression(viewNode);
            MvUtils.deriveLogicalProperty(query);
            List<ColumnRefOperator> outputs = query.getOutputColumns().getColumnRefOperators(factory);
            ColumnRefOperator source = outputs.stream().filter(col -> col.getName().equals("source"))
                    .findFirst().orElseThrow();
            List<ColumnRefOperator> required = outputs.stream()
                    .filter(col -> col.getName().equals("id") || (window && !col.equals(source))).toList();
            Assertions.assertFalse(required.isEmpty());
            if (join) {
                QueryStatement right = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                        "SELECT id FROM t", connectContext);
                var rightPlan = new RelationTransformer(factory, connectContext).transform(right.getQueryRelation());
                query = OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null),
                        query, rightPlan.getRoot());
            }
            var context = OptimizerFactory.initContext(connectContext, factory);
            context.setTaskContext(new TaskContext(context, new PhysicalPropertySet(),
                    new ColumnRefSet(required), Double.MAX_VALUE));
            var mv = starRocksAssert.getMv(database, "mv");
            var base = starRocksAssert.getTable(database, "t");
            var identity = new ReplaceColumnRefRewriter(Map.of());
            var queryPredicates = PredicateSplit.splitPredicate(ConstantOperator.TRUE);
            var coverage = new BinaryPredicateOperator(BinaryType.LT, source, ConstantOperator.createInt(10));
            var materialization = new MaterializationContext(context, mv, logical.getRoot(), factory, factory,
                    List.of(base), List.of(base), MvUpdateInfo.noRefresh(mv), required, 0);
            var mvRewrite = new MvRewriteContext(materialization, MvUtils.getAllTables(query), query, identity,
                    queryPredicates, List.of(), join ? OnlyJoinRule.getInstance() : OnlyScanRule.getInstance());
            var rewriteContext = new RewriteContext(query, queryPredicates, new EquivalenceClasses(), Map.of(),
                    factory, identity, logical.getRoot(), PredicateSplit.splitPredicate(coverage), Map.of(),
                    factory, identity, Map.of(), Set.copyOf(factory.getColumnRefs()), context);
            // Exercise compensation on an operator obtained from a SQL view, then ordinary column pruning.
            // This isolates the rewriter contract without claiming the full MV candidate path accepts this view.
            OptExpression compensated = new MaterializedViewRewriter(mvRewrite)
                    .doQueryBasedRewrite(rewriteContext, coverage, query);
            Assertions.assertNotNull(compensated);
            var planning = OptimizerFactory.initContext(connectContext, factory, new OptimizerOptions());
            OptExpression physical = OptimizerFactory.create(planning).optimize(
                    compensated, new PhysicalPropertySet(), new ColumnRefSet(required));
            Assertions.assertTrue(nodes(physical).stream()
                    .filter(node -> !(node.getOp() instanceof PhysicalProjectOperator))
                    .flatMap(node -> Utils.extractConjuncts(node.getOp().getPredicate()).stream())
                    .anyMatch(predicate -> predicate instanceof BinaryPredicateOperator binary
                            && binary.getBinaryType() == BinaryType.GE
                            && binary.getChild(0) instanceof ColumnRefOperator column
                            && column.getName().equals("source")
                            && binary.getChild(1).equals(ConstantOperator.createInt(10))),
                    () -> "The exclusion must survive pruning even though source is not selected\n" + physical.debugString());
        } finally {
            try {
                starRocksAssert.dropDatabase(database);
            } finally {
                connectContext.setDatabase(previousDatabase);
                connectContext.setSessionVariable(previousSession);
            }
        }
    }

    private static List<OptExpression> nodes(OptExpression root) {
        List<OptExpression> nodes = new ArrayList<>(List.of(root));
        for (int i = 0; i < nodes.size(); i++) {
            nodes.addAll(nodes.get(i).getInputs());
        }
        return nodes;
    }
}
