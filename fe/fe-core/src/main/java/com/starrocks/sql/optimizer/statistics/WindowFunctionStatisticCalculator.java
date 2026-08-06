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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class WindowFunctionStatisticCalculator {
    private WindowFunctionStatisticCalculator() {
    }

    public static ColumnStatistic calculate(ScalarOperator operator, Statistics inputStatistics,
                                            List<ScalarOperator> partitionExpressions) {
        double avgRowsPerPartition = estimateAvgRowsPerPartition(inputStatistics, partitionExpressions);
        return ExpressionStatisticCalculator.calculateForWindow(operator, inputStatistics, avgRowsPerPartition);
    }

    @VisibleForTesting
    static double estimateAvgRowsPerPartition(Statistics inputStatistics, List<ScalarOperator> partitionExpressions) {
        double inputRowCount = inputStatistics.getOutputRowCount();
        if (partitionExpressions.isEmpty()) {
            return Math.max(1, inputRowCount);
        }
        List<ColumnRefOperator> partitionCols = Lists.newArrayList();
        for (ScalarOperator expr : partitionExpressions) {
            if (!(expr instanceof ColumnRefOperator column)) {
                return Math.max(1, inputRowCount);
            }
            partitionCols.add(column);
        }
        double numPartitions = StatisticsCalculator.computeGroupByStatistics(partitionCols, inputStatistics,
                Maps.newHashMap());
        return Math.max(1, inputRowCount / numPartitions);
    }
}
