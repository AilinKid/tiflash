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


#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/SettingsCommon.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <common/ThreadPool.h>

#include <shared_mutex>

namespace DB
{
/// groupingSets are formed as { groupingSet, groupingSet...}
/// groupingSet are formed as { groupingExprs, groupingExprs...}
/// groupingExprs are formed as slice of expression/column names
/// simply for now case like: select count(distinct a), count(distinct b) from t;
///     we got 2 groupings set like: {[<a>], [<b>]}
///
/// shortly soon, we can support the grouping sets merging, which could take case
/// like: select count(distinct a,b), count(distinct a), count(distinct c) from t as
///     we still got 2 grouping sets like: {[<a>, <a,b>], [<c>]}
///
/// the second case in which the group layout <a,b> has been merged with the prefix
/// common group layout <a> into unified one set to reduce the underlying data replication/repeat cost.
///
using GroupingColumnName = ::String;
using GroupingColumnNames = std::vector<GroupingColumnName>;
using GroupingSet = std::vector<GroupingColumnNames>;
using GroupingSets = std::vector<GroupingSet>;



/** Data structure for implementation of Repeat.
  *
  * Repeat is a kind of operator used for replicate low-layer datasource rows to feed different aggregate
  * grouping-layout requirement. (Basically known as grouping sets)
  *
  * For current scenario, it is applied to accelerate the computation of multi distinct aggregates by utilizing
  * multi nodes computing resource in a way of scheming 3-phase aggregation under mpp mode.
  *
  * GroupingSets descriptions are all needed by Repeat operator itself, the length of GroupingSets are the needed
  * repeat number (in other words, one grouping set require one replica of source rows). Since different grouping
  * set column shouldn't let its targeted rows affected by other grouping set columns (which will also be appear in
  * the group by items) when do grouping work, we should isolate different grouping set columns by filling them with
  * null values when repeating rows.
  *
  * Here is an example:
  * Say we got a query like this:                   select count(distinct a), count(distinct b) from t.
  *
  * Downward requirements formed by this query are consist of two different grouping set <a>, <b>, and both of this
  * two columns will be in the group by items. Make record here as ---  GROUP BY(a,b)
  *
  * Different group layouts are doomed to be unable to be feed with same replica of data in shuffling mode Except
  * gathering them all to the single node. While the latter one is usually accompanied by a single point of bottleneck.
  *
  * That's why data repeat happens here. Say we got two tuple as below:
  *
  * <a>     <b>         ==> after repeat we got            <a>    <b>
  *  1       1                                origin row    1      1
  *  1       2                                repeat row    1      1
  *                                           origin row    1      2
  *                                           repeat row    1      2
  *
  * See what we got now above, although we have already repeated/doubled the origin rows, while when grouping them together
  * with GROUP BY(a,b) clause (resulting 2 group (1,1),(1,2) here), we found that we still can not get the right answer for
  * count distinct agg for a.
  *
  * From the theory, every origin/repeated row should be targeted for one group out requirement, which means row<1> and row<3>
  * about should be used to feed count(distinct a), while since the value of b in row<3> is different from that from row<1>,
  * that leads them being divided into different group.
  *
  * Come back to the origin goal to feed count(distinct a), in which we don't even care about what is was in column b from row<1>
  * and row<3>, because current agg args is aimed at column a. Therefore, we filled every non-targeted grouping set column in
  * repeated row as null value. After that we got as below:
  *
  * <a>     <b>         ==> after repeat we got            <a>    <b>
  *  1       1                                origin row    1     null         ---> target for grouping set a
  *  1       2                                repeat row   null    1           ---> target for grouping set b
  *                                           origin row    1     null         ---> target for grouping set a
  *                                           repeat row   null    2           ---> target for grouping set b
  *
  * Then, when grouping them together with GROUP BY(a,b) clause, we got row<1> and row<3> together, and row<2>, row<4> as a
  * self-group individually. Among them, every distinct agg has their self-targeted data grouped correctly. GROUP BY(a,b) clause
  * is finally seen/taken as a equivalent group to GROUP BY(a, null) for a-targeted rows, GROUP BY(null, b) for b-targeted rows.
  *
  * Over the correct grouped data, the result computation for distinct agg is quite reasonable. By the way, if origin row has some
  * column that isn't belong to any grouping set, just let it be copied as it was in repeated row.
  *
  */
class Repeat
{
public:
    explicit Repeat(const GroupingSets & gss);

    // replicateAndFillNull is the basic functionality that Repeat Operator provided. Briefly, it replicates
    // origin rows with regard to local grouping sets description, and appending a new column named as groupingID
    // to illustrate what group this row is targeted for.
    void replicateAndFillNull(Block & input) const;

    size_t getGroupSetNum() const {return group_sets_names.size();}

    bool isInGroupSetColumn(String name) const;

    const GroupingColumnNames& getGroupSetColumnNamesByOffset(size_t offset) const;

    void getAllGroupSetColumnNames(std::set<String>& name_set) const;

    static std::shared_ptr<Repeat> sharedRepeat(const GroupingSets & groupingSets);

    void getGroupingSetsDes(FmtBuffer & buffer) const;

    static const String grouping_identifier_column_name;

    static const DataTypePtr grouping_identifier_column_type;

private:
    GroupingSets group_sets_names;
};
} // namespace DB