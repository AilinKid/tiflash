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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class FilterExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver("exchange1",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"})});
    }
};

TEST_F(FilterExecutorTestRunner, equals)
try
{
    auto request = context
                       .scan("test_db", "test_table")
                       .filter(eq(col("s1"), col("s2")))
                       .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"banana"})});

    request = context.receive("exchange1")
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"banana"})});

    request = context.receive("exchange1")
                  .filter(eq(col("s1"), col("s1")))
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", "banana"}),
         toNullableVec<String>({"apple", "banana"})});

    request = context.receive("exchange1")
                  .filter(eq(col("s1"), lit(Field(String("0")))))
                  .build(context);
    executeAndAssertColumnsEqual(request, {});
}
CATCH

TEST_F(FilterExecutorTestRunner, const_bool)
try
{
    auto const_true = lit(Field(static_cast<UInt64>(1)));
    {
        auto request = context.receive("exchange1")
                           .filter(const_true)
                           .build(context);
        executeAndAssertColumnsEqual(
            request,
            {toNullableVec<String>({"banana", {}, "banana"}),
             toNullableVec<String>({"apple", {}, "banana"})});
    }

    auto const_false = lit(Field(static_cast<UInt64>(0)));
    {
        auto request = context.receive("exchange1")
                           .filter(const_false)
                           .build(context);
        executeAndAssertColumnsEqual(request, {});
    }

    auto column_not_null_true = eq(col("s1"), col("s1"));
    auto column_false = eq(col("s1"), lit(Field(String("0"))));
    auto column_other = eq(col("s1"), col("s2"));

    auto test_and = [&](const ASTPtr & a, const ASTPtr & b, const ColumnsWithTypeAndName & expect_columns) {
        auto request = context.receive("exchange1")
                           .filter(And(a, b))
                           .build(context);
        executeAndAssertColumnsEqual(request, expect_columns);

        request = context.receive("exchange1")
                      .filter(And(b, a))
                      .build(context);
        executeAndAssertColumnsEqual(request, expect_columns);
    };

    test_and(const_true, column_not_null_true, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_and(const_true, column_false, {});
    test_and(const_true, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    test_and(const_false, column_not_null_true, {});
    test_and(const_false, column_false, {});
    test_and(const_false, column_other, {});

    auto test_or = [&](const ASTPtr & a, const ASTPtr & b, const ColumnsWithTypeAndName & expect_columns) {
        auto request = context.receive("exchange1")
                           .filter(Or(a, b))
                           .build(context);
        executeAndAssertColumnsEqual(request, expect_columns);

        request = context.receive("exchange1")
                      .filter(Or(b, a))
                      .build(context);
        executeAndAssertColumnsEqual(request, expect_columns);
    };

    test_or(const_true, column_not_null_true, {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_or(const_true, column_false, {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_or(const_true, column_other, {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});

    test_or(const_false, column_not_null_true, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(const_false, column_false, {});
    test_or(const_false, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
}
CATCH

TEST_F(FilterExecutorTestRunner, FilterWithQualifiedFormat)
try
{
    auto request = context
                       .scan("test_db", "test_table")
                       .filter(eq(col("test_table.s1"), col("test_table.s2")))
                       .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana"}),
         toNullableVec<String>({"banana"})});
}
CATCH

TEST_F(FilterExecutorTestRunner, RepeatLogical)
try
{
    /// following tests is ok now for non-planner enabled.

    /// case 1
    auto request = context
                  .scan("test_db", "test_table")
                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"s1"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///    s1       s2      groupingID
    ///  "banana"  NULL         1
    ///   NULL    "apple"       2
    ///   NULL     NULL         1
    ///   NULL     NULL         2
    ///  "banana"  NULL         1
    ///   NULL   "banana"       2
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", {}, {}, {}, "banana", {}}),
         toNullableVec<String>({{}, "apple", {}, {}, {}, "banana"}),
         toVec<UInt64>({1,2,1,2,1,2})});

    /// case 2
    request = context
                  .scan("test_db", "test_table")
                  .filter(eq(col("s1"), col("s2")))
                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"s1"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///    s1       s2
    /// "banana"  "banana"
    ///          |
    ///          v
    ///    s1       s2      groupingID
    ///  "banana"  NULL         1
    ///   NULL   "banana"       2
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", {}}),
         toNullableVec<String>({{}, "banana"}),
         toVec<UInt64>({1,2})});

    /// case 3
    request = context
                  .scan("test_db", "test_table")
                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"s1"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    /// data flow: TiFlash isn't aware of the operation sequence, this filter here will be run before repeat does just like the second test case above.
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", {}}),
         toNullableVec<String>({{}, "banana"}),
         toVec<UInt64>({1,2})});

    /// case 4
    auto const_false = lit(Field(static_cast<UInt64>(0)));
    request = context
                  .scan("test_db", "test_table")
                  .filter(const_false)                      // refuse all rows
                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"s1"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {});

    /// case 5   (test integrated with aggregation)
    request = context
                  .scan("test_db", "test_table")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"count(s1)"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
                  .build(context);
    /// data flow:
    ///
    ///    s1       s2
    /// "banana"  "apple"
    ///   NULL      NULL
    /// "banana"  "banana"
    ///          |
    ///          v
    ///  count(s1)   s2
    ///    1      "apple"
    ///    0       NULL
    ///    1      "banana"
    ///          |
    ///          v
    ///  count(s1)   s2      groupingID
    ///    1        NULL        1
    ///   NULL     "apple"      2
    ///    0        NULL        1
    ///   NULL      NULL        2
    ///    1        NULL        1
    ///   NULL     "banana"     2
    ///
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({1, {}, 0, {}, 1,{}}),
            toNullableVec<String>({{}, "apple", {},{},{}, "banana"}),
                toVec<UInt64>({1,2,1,2,1,2})});

    /// case 5   (test integrated with aggregation and projection)
    request = context
                  .scan("test_db", "test_table")
                  .aggregation({Count(col("s1"))}, {col("s2")})
                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"count(s1)"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
                  .project({"count(s1)"})
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({1, {}, 0, {}, 1,{}})});

    /// case 6   (test integrated with aggregation and projection and limit)
    /// note: by now, limit is executed before repeat does to reduce unnecessary row repeat work.
//    request = context
//                  .scan("test_db", "test_table")
//                  .aggregation({Count(col("s1"))}, {col("s2")})
//                  .repeat(MockVVecColumnNameVec{MockVecColumnNameVec{MockColumnNameVec{"count(s1)"},}, MockVecColumnNameVec{MockColumnNameVec{"s2"},},})
//                  .project({"count(s1)"})
//                  .limit(2)
//                  .build(context);
//    executeAndAssertColumnsEqual(
//        request,
//        {toNullableVec<UInt64>({1, {}, 0, {}})});

}
CATCH

/// TODO: more functions.

} // namespace tests
} // namespace DB
