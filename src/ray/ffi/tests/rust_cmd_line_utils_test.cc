// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Rust FFI tests for command line utilities.
// Matches: src/ray/util/tests/cmd_line_utils_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_cmd_line_utils.h"

#include <string>
#include <vector>

namespace ray {

TEST(RustCmdLineUtilsTest, ParseCommandLineTest) {
  typedef std::vector<std::string> ArgList;
  CommandLineSyntax posix = CommandLineSyntax::POSIX, win32 = CommandLineSyntax::Windows,
                    all[] = {posix, win32};
  for (CommandLineSyntax syn : all) {
    ASSERT_EQ(ParseCommandLine(R"(aa)", syn), ArgList({R"(aa)"}));
    ASSERT_EQ(ParseCommandLine(R"(a )", syn), ArgList({R"(a)"}));
    ASSERT_EQ(ParseCommandLine(R"(\" )", syn), ArgList({R"(")"}));
    ASSERT_EQ(ParseCommandLine(R"(" a")", syn), ArgList({R"( a)"}));
    ASSERT_EQ(ParseCommandLine(R"("\\")", syn), ArgList({R"(\)"}));
    ASSERT_EQ(ParseCommandLine(/*R"("\"")"*/ "\"\\\"\"", syn), ArgList({R"(")"}));
    ASSERT_EQ(ParseCommandLine(R"(a" b c"d )", syn), ArgList({R"(a b cd)"}));
    ASSERT_EQ(ParseCommandLine(R"(\"a b)", syn), ArgList({R"("a)", R"(b)"}));
    ASSERT_EQ(ParseCommandLine(R"(| ! ^ # [)", syn), ArgList({"|", "!", "^", "#", "["}));
    ASSERT_EQ(ParseCommandLine(R"(; ? * $ &)", syn), ArgList({";", "?", "*", "$", "&"}));
    ASSERT_EQ(ParseCommandLine(R"(: ` < > ~)", syn), ArgList({":", "`", "<", ">", "~"}));
  }
  ASSERT_EQ(ParseCommandLine(R"( a)", posix), ArgList({R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"( a)", win32), ArgList({R"()", R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\ a)", posix), ArgList({R"( a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\ a)", win32), ArgList({R"(\)", R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\ D)", posix), ArgList({R"(C: D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\ D)", win32), ArgList({R"(C:\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\ D)", posix), ArgList({R"(C:\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\ D)", win32), ArgList({R"(C:\\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\  D)", posix), ArgList({R"(C: )", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\  D)", win32), ArgList({R"(C:\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\\  D)", posix), ArgList({R"(C:\ )", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(C:\\\  D)", win32), ArgList({R"(C:\\\)", R"(D)"}));
  ASSERT_EQ(ParseCommandLine(R"(\)", posix), ArgList({R"()"}));
  ASSERT_EQ(ParseCommandLine(R"(\)", win32), ArgList({R"(\)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\a)", posix), ArgList({R"(\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\a)", win32), ArgList({R"(\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\\a)", posix), ArgList({R"(\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\\a)", win32), ArgList({R"(\\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\)", posix), ArgList({R"(\)"}));
  ASSERT_EQ(ParseCommandLine(R"(\\)", win32), ArgList({R"(\\)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\a")", posix), ArgList({R"(\a)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\a")", win32), ArgList({R"(\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\\a")", posix), ArgList({R"(\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"("\\\a")", win32), ArgList({R"(\\\a)"}));
  ASSERT_EQ(ParseCommandLine(R"('a'' b')", posix), ArgList({R"(a b)"}));
  ASSERT_EQ(ParseCommandLine(R"('a'' b')", win32), ArgList({R"('a'')", R"(b')"}));
  ASSERT_EQ(ParseCommandLine(R"('a')", posix), ArgList({R"(a)"}));
  ASSERT_EQ(ParseCommandLine(R"('a')", win32), ArgList({R"('a')"}));
  ASSERT_EQ(ParseCommandLine(R"(x' a \b')", posix), ArgList({R"(x a \b)"}));
  ASSERT_EQ(ParseCommandLine(R"(x' a \b')", win32), ArgList({R"(x')", R"(a)", R"(\b')"}));
}

TEST(RustCmdLineUtilsTest, CreateCommandLineTest) {
  typedef std::vector<std::string> ArgList;
  CommandLineSyntax posix = CommandLineSyntax::POSIX, win32 = CommandLineSyntax::Windows,
                    all[] = {posix, win32};
  std::vector<ArgList> test_cases({
      ArgList({R"(a)"}),
      ArgList({R"(a b)"}),
      ArgList({R"(")"}),
      ArgList({R"(')"}),
      ArgList({R"(\)"}),
      ArgList({R"(/)"}),
      ArgList({R"(#)"}),
      ArgList({R"($)"}),
      ArgList({R"(!)"}),
      ArgList({R"(@)"}),
      ArgList({R"(`)"}),
      ArgList({R"(&)"}),
      ArgList({R"(|)"}),
      ArgList({R"(a")", R"('x)", R"(?'"{)", R"(]))", R"(!)", R"(~`\)"}),
  });
  for (CommandLineSyntax syn : all) {
    for (const ArgList &arglist : test_cases) {
      // Test roundtrip: CreateCommandLine -> ParseCommandLine should give back the original
      ASSERT_EQ(ParseCommandLine(CreateCommandLine(arglist, syn), syn), arglist);
    }
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
