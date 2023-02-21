// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalExpand.h>
#include <Interpreters/Context.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/FilterTransformOp.h>
#include <fmt/format.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalExpand::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Expand & expand,
    const PhysicalPlanNodePtr & child)
{
    assert(child);

    child->finalize();

    if (unlikely(expand.grouping_sets().empty()))
    {
        //should not reach here
        throw TiFlashException("Expand executor without grouping sets", Errors::Planner::BadRequest);
    }

    DAGExpressionAnalyzer analyzer{child->getSchema(), context};
    ExpressionActionsPtr before_expand_actions = PhysicalPlanHelper::newActions(child->getSampleBlock());

    auto grouping_sets = analyzer.buildExpandGroupingColumns(expand, before_expand_actions);
    auto expand_action = ExpressionAction::expandSource(grouping_sets);
    // include expand action itself.
    before_expand_actions->add(expand_action);

    // construct sample block.
    NamesAndTypes expand_output_columns;
    auto child_header = child->getSchema();
    for (const auto & one : child_header)
    {
        expand_output_columns.emplace_back(one.name, expand_action.expand->isInGroupSetColumn(one.name) ? makeNullable(one.type) : one.type);
    }
    expand_output_columns.emplace_back(expand_action.expand->grouping_identifier_column_name, expand_action.expand->grouping_identifier_column_type);

    auto physical_expand = std::make_shared<PhysicalExpand>(
        executor_id,
        expand_output_columns,
        log->identifier(),
        child,
        expand_action.expand,
        before_expand_actions,
        Block(expand_output_columns));

    return physical_expand;
}


void PhysicalExpand::expandTransform(DAGPipeline & child_pipeline)
{
    String expand_extra_info = fmt::format("expand, expand_executor_id = {}: grouping set {}", execId(), shared_expand->getGroupingSetsDes());
    child_pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, expand_actions, log->identifier());
        stream->setExtraInfo(expand_extra_info);
    });
}

void PhysicalExpand::buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context &, size_t)
{
    auto input_header = group_builder.getCurrentHeader();
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(group_builder.exec_status, expand_actions, log->identifier()));
    });
}

void PhysicalExpand::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);
    expandTransform(pipeline);
}

void PhysicalExpand::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    child->finalize(expand_actions->getRequiredColumns());
}

const Block & PhysicalExpand::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
