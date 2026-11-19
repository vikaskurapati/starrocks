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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.IMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyScanRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.validate.InputDependenciesChecker;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Exercises the query branch of MV UNION compensation directly. Partial-refresh view candidates can
 * be rejected by the full planner before this call, so this test isolates the project-root contract
 * used by OnlyScanRule and AggregateScanRule without mocking predicate construction or project merging.
 */
public class ViewUnionRewriteCompensationTest extends PlanTestNoneDBBase {
    private static final String DATABASE = "view_union_compensation_repro";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testProjectedViewScanPreservesUnionCompensation() throws Exception {
        assertCompensationSurvivesPlanning(false, ProjectionKind.IDENTITY);
    }

    @Test
    public void testAggregateOverProjectedViewScanPreservesUnionCompensation() throws Exception {
        assertCompensationSurvivesPlanning(true, ProjectionKind.IDENTITY);
    }

    @Test
    public void testAliasedViewScanPreservesUnionCompensation() throws Exception {
        assertCompensationSurvivesPlanning(false, ProjectionKind.ALIAS);
    }

    @Test
    public void testAggregateOverAliasedViewScanPreservesUnionCompensation() throws Exception {
        assertCompensationSurvivesPlanning(true, ProjectionKind.ALIAS);
    }

    @Test
    public void testComputedViewProjectionPreservesUnionCompensation() throws Exception {
        assertCompensationSurvivesPlanning(false, ProjectionKind.COMPUTED);
    }

    @Test
    public void testAggregateOverComputedViewProjectionPreservesUnionCompensation() throws Exception {
        assertCompensationSurvivesPlanning(true, ProjectionKind.COMPUTED);
    }

    @Test
    public void testJoinOverProjectedViewsPreservesUnionCompensation() throws Exception {
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableMaterializedViewTextMatchRewrite(false);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
            starRocksAssert.withTable("CREATE TABLE t (id INT NOT NULL, source INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS SELECT id, source FROM t");
            starRocksAssert.withView("CREATE VIEW w AS SELECT id, source FROM t");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') "
                    + "AS SELECT a.id FROM v a JOIN w b ON a.id = b.id WHERE a.id < 10");
            ColumnRefFactory factory = new ColumnRefFactory();
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "SELECT a.id, b.id FROM v a JOIN w b ON a.id = b.id", connectContext);
            LogicalPlan logical = new RelationTransformer(factory, connectContext)
                    .transform(statement.getQueryRelation());
            List<OptExpression> viewPlans = planNodes(logical.getRoot()).stream()
                    .filter(node -> node.getOp().getEquivalentOp() instanceof LogicalViewScanOperator).toList();
            Assertions.assertEquals(2, viewPlans.size());
            List<OptExpression> viewScans = new ArrayList<>();
            for (int i = 0; i < viewPlans.size(); i++) {
                OptExpression inline = viewPlans.get(i);
                LogicalViewScanOperator view = (LogicalViewScanOperator) inline.getOp().getEquivalentOp();
                ColumnRefOperator id = logical.getOutputColumn().get(i);
                view.setOriginalPlanEvaluator(new LogicalViewScanOperator.OptPlanEvaluator(
                        MvUtils.cloneExpression(inline), connectContext, view.getOutputColumnSet(), factory));
                viewScans.add(OptExpression.create(LogicalViewScanOperator.builder().withOperator(view)
                        .setProjection(new Projection(Map.of(id, view.getProjection().getColumnRefMap().get(id))))
                        .build()));
            }
            ColumnRefOperator leftId = logical.getOutputColumn().get(0);
            ColumnRefOperator rightId = logical.getOutputColumn().get(1);
            OptExpression query = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                    new BinaryPredicateOperator(BinaryType.EQ, leftId, rightId)), viewScans);
            OptimizerContext context = OptimizerFactory.initContext(connectContext, factory);
            context.setTaskContext(new TaskContext(context, new PhysicalPropertySet(),
                    new ColumnRefSet(List.of(leftId)), Double.MAX_VALUE));
            MaterializedView mv = starRocksAssert.getMv(DATABASE, "mv");
            Table base = starRocksAssert.getTable(DATABASE, "t");
            PredicateSplit queryPredicates = PredicateSplit.splitPredicate(ConstantOperator.TRUE);
            ScalarOperator coveredRows = new BinaryPredicateOperator(BinaryType.LT, leftId,
                    ConstantOperator.createInt(10));
            ReplaceColumnRefRewriter identity = new ReplaceColumnRefRewriter(Map.of());
            MaterializationContext materialization = new MaterializationContext(context, mv, logical.getRoot(),
                    factory, factory, List.of(base), List.of(base), MvUpdateInfo.noRefresh(mv), List.of(leftId), 0);
            MvRewriteContext mvRewriteContext = new MvRewriteContext(materialization, MvUtils.getAllTables(query),
                    query, identity, queryPredicates, List.of(), OnlyJoinRule.getInstance());
            RewriteContext rewriteContext = new RewriteContext(query, queryPredicates, new EquivalenceClasses(),
                    Map.of(), factory, identity, logical.getRoot(), PredicateSplit.splitPredicate(coveredRows),
                    Map.of(), factory, identity, Map.of(), Set.copyOf(factory.getColumnRefs()), context);
            OptExpression compensated = new MaterializedViewRewriter(mvRewriteContext)
                    .doQueryBasedRewrite(rewriteContext, coveredRows, query);
            Assertions.assertNotNull(compensated);

            session.setEnableMaterializedViewRewrite(false);
            OptimizerContext planningContext = OptimizerFactory.initContext(connectContext, factory);
            OptExpression physical = OptimizerFactory.create(planningContext).optimize(
                    compensated, new PhysicalPropertySet(), new ColumnRefSet(List.of(leftId)));
            InputDependenciesChecker.getInstance().validate(physical, planningContext.getTaskContext());
            Assertions.assertTrue(planNodes(physical).stream()
                            .flatMap(node -> Utils.extractConjuncts(node.getOp().getPredicate()).stream())
                            .anyMatch(predicate -> excludesCoveredRows(predicate, leftId)
                                    || excludesCoveredRows(predicate, rightId)),
                    () -> "Join compensation lost id >= 10 from its projected view children.\n"
                            + physical.debugString());
        } finally {
            starRocksAssert.dropDatabase(DATABASE);
            connectContext.setDatabase(previousDatabase);
            connectContext.setSessionVariable(previousSession);
        }
    }

    private enum ProjectionKind {
        IDENTITY, ALIAS, COMPUTED
    }

    private void assertCompensationSurvivesPlanning(boolean aggregate, ProjectionKind projectionKind) throws Exception {
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = (SessionVariable) previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setEnableMaterializedViewTextMatchRewrite(false);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
            starRocksAssert.withTable("CREATE TABLE t (id INT NOT NULL, source INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS SELECT id, source FROM t");
            // The MV covers only values below 10; its UNION query branch must exclude those rows.
            // A deferred MV supplies view metadata, but no refresh or BE execution is needed here.
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') "
                    + "AS SELECT id, source FROM v WHERE "
                    + (projectionKind == ProjectionKind.COMPUTED ? "abs(id)" : "id") + " < 10");

            ColumnRefFactory factory = new ColumnRefFactory();
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "SELECT id FROM v", connectContext);
            LogicalPlan logicalPlan = new RelationTransformer(factory, connectContext)
                    .transform(statement.getQueryRelation());
            OptExpression inlineView = planNodes(logicalPlan.getRoot()).stream()
                    .filter(node -> node.getOp().getEquivalentOp() instanceof LogicalViewScanOperator)
                    .findFirst().orElseThrow();
            LogicalViewScanOperator view = (LogicalViewScanOperator) inlineView.getOp().getEquivalentOp();
            ColumnRefOperator id = logicalPlan.getOutputColumn().get(0);
            ScalarOperator viewId = view.getProjection().getColumnRefMap().get(id);
            Assertions.assertNotNull(viewId);
            ColumnRefOperator output = projectionKind == ProjectionKind.IDENTITY
                    ? id : factory.create("projected_id", id.getType(), id.isNullable());
            ScalarOperator projectedValue = viewId;
            ScalarOperator inputValue = id;
            if (projectionKind == ProjectionKind.COMPUTED) {
                Function abs = ExprUtils.getBuiltinFunction(
                        FunctionSet.ABS, new Type[] {id.getType()}, Function.CompareMode.IS_IDENTICAL);
                Assertions.assertNotNull(abs);
                projectedValue = new CallOperator(FunctionSet.ABS, id.getType(), List.of(viewId), abs);
                inputValue = new CallOperator(FunctionSet.ABS, id.getType(), List.of(id), abs);
            }
            final ScalarOperator expectedInputValue = inputValue;

            view.setOriginalPlanEvaluator(new LogicalViewScanOperator.OptPlanEvaluator(
                    MvUtils.cloneExpression(inlineView), connectContext, view.getOutputColumnSet(), factory));
            LogicalViewScanOperator projectedView = LogicalViewScanOperator.builder()
                    .withOperator(view)
                    .setProjection(new Projection(Map.of(output, projectedValue)))
                    .build();
            OptExpression query = OptExpression.create(projectedView);
            Assertions.assertNotNull(query.getOp().getProjection(),
                    "The matched input must be a view scan with an attached projection");
            if (aggregate) {
                // A grouping-only aggregate exercises AggregateScanRule's inputAt(0) delegation.
                query = OptExpression.create(new LogicalAggregationOperator(
                        AggType.GLOBAL, List.of(output), Map.<ColumnRefOperator, CallOperator>of()), query);
            }

            OptimizerContext context = OptimizerFactory.initContext(connectContext, factory);
            context.setTaskContext(new TaskContext(
                    context, new PhysicalPropertySet(), new ColumnRefSet(List.of(output)), Double.MAX_VALUE));
            MaterializedView mv = starRocksAssert.getMv(DATABASE, "mv");
            Table baseTable = starRocksAssert.getTable(DATABASE, "t");
            ReplaceColumnRefRewriter identity = new ReplaceColumnRefRewriter(Map.of());
            PredicateSplit queryPredicates = PredicateSplit.splitPredicate(ConstantOperator.TRUE);
            ScalarOperator coveredRows = new BinaryPredicateOperator(BinaryType.LT, output, ConstantOperator.createInt(10));
            PredicateSplit mvPredicates = PredicateSplit.splitPredicate(coveredRows);
            MaterializationContext materialization = new MaterializationContext(context, mv, inlineView,
                    factory, factory, List.of(baseTable), List.of(baseTable),
                    MvUpdateInfo.noRefresh(mv), List.of(id), 0);
            MvRewriteContext mvRewriteContext = new MvRewriteContext(materialization, List.of(view.getTable()),
                    query, identity, queryPredicates, List.of(),
                    aggregate ? AggregateScanRule.getInstance() : OnlyScanRule.getInstance());
            RewriteContext rewriteContext = new RewriteContext(query, queryPredicates, new EquivalenceClasses(),
                    Map.of(), factory, identity, inlineView, mvPredicates, Map.of(), factory, identity,
                    Map.of(), Set.copyOf(factory.getColumnRefs()), context);
            IMaterializedViewRewriter rewriter = aggregate
                    ? new AggregatedMaterializedViewRewriter(mvRewriteContext)
                    : new MaterializedViewRewriter(mvRewriteContext);

            OptExpression compensated = rewriter.doQueryBasedRewrite(rewriteContext, coveredRows, query);
            Assertions.assertNotNull(compensated, "The query compensation branch must be constructed");
            Assertions.assertTrue(planNodes(compensated).stream()
                            .flatMap(node -> Utils.extractConjuncts(node.getOp().getPredicate()).stream())
                            .anyMatch(predicate -> excludesCoveredRows(predicate, output)),
                    "The rewriter must construct the exclusion before the optimizer processes the branch");
            OptExpression queryBranch = aggregate ? compensated.inputAt(0) : compensated;
            Assertions.assertInstanceOf(LogicalFilterOperator.class, queryBranch.getOp(),
                    "A Project cannot evaluate an attached predicate");
            Assertions.assertInstanceOf(LogicalProjectOperator.class, queryBranch.inputAt(0).getOp());
            Assertions.assertNull(queryBranch.inputAt(0).getOp().getPredicate());
            Assertions.assertTrue(queryBranch.inputAt(0).getOutputColumns()
                            .containsAll(queryBranch.getOp().getPredicate().getUsedColumns()),
                    "The filter must reference the Project's output, before alias/expression substitution");
            InputDependenciesChecker.getInstance().validate(compensated, context.getTaskContext());

            // Run the ordinary optimizer on the returned query branch. The exclusion must survive
            // project merging and physical implementation, not merely exist on a logical project.
            session.setEnableMaterializedViewRewrite(false);
            OptimizerContext planningContext = OptimizerFactory.initContext(
                    connectContext, factory, new OptimizerOptions());
            OptExpression physical = OptimizerFactory.create(planningContext).optimize(
                    compensated, new PhysicalPropertySet(), new ColumnRefSet(List.of(output)));
            InputDependenciesChecker.getInstance().validate(physical, planningContext.getTaskContext());
            Assertions.assertTrue(planNodes(physical).stream()
                            .filter(node -> !(node.getOp() instanceof PhysicalProjectOperator))
                            .flatMap(node -> Utils.extractConjuncts(node.getOp().getPredicate()).stream())
                            .anyMatch(predicate -> excludesCoveredRows(predicate, expectedInputValue)),
                    () -> "The query branch lost the exclusion for " + expectedInputValue
                            + " >= 10 and would overlap the MV's rows.\n"
                            + physical.debugString());
        } finally {
            try {
                starRocksAssert.dropDatabase(DATABASE);
            } finally {
                connectContext.setDatabase(previousDatabase);
                connectContext.setSessionVariable(previousSession);
            }
        }
    }

    private static boolean excludesCoveredRows(ScalarOperator predicate, ScalarOperator value) {
        return predicate instanceof BinaryPredicateOperator binary
                && binary.getBinaryType() == BinaryType.GE
                && binary.getChild(0).equals(value)
                && binary.getChild(1).equals(ConstantOperator.createInt(10));
    }

    private static List<OptExpression> planNodes(OptExpression root) {
        List<OptExpression> nodes = new ArrayList<>(List.of(root));
        for (int i = 0; i < nodes.size(); i++) {
            nodes.addAll(nodes.get(i).getInputs());
        }
        return nodes;
    }
}
