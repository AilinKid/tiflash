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

#include <DataStreams//RepeatSourceBlockInputStream.h>
namespace DB
{
Block RepeatSourceBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (!block)
        return block;
    repeat_source_actions->execute(block);
    return block;
}

Block RepeatSourceBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    repeat_source_actions->execute(res);
    return res;
}

void RepeatSourceBlockInputStream::appendInfo(FmtBuffer & buffer) const {
    buffer.fmtAppend(": grouping set ");
    repeat_source_actions.get()->getActions()[0].repeat->getGroupingSetsDes(buffer);
}

} // namespace DB