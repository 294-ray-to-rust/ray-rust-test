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

//! Command line parsing utilities FFI bridge.
//!
//! Provides ParseCommandLine and CreateCommandLine with POSIX and Windows
//! syntax support.

/// Command line syntax type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandLineSyntax {
    POSIX,
    Windows,
}

/// Parse a command line string into a vector of arguments.
///
/// Supports both POSIX and Windows escaping/quoting conventions.
pub fn parse_command_line(cmdline: &str, syntax: CommandLineSyntax) -> Vec<String> {
    let mut args = Vec::new();
    let mut current_arg = String::new();
    let chars: Vec<char> = cmdline.chars().collect();
    let mut i = 0;
    let mut in_double_quote = false;
    let mut in_single_quote = false;

    while i < chars.len() {
        let c = chars[i];

        if c == '\\' {
            if syntax == CommandLineSyntax::POSIX {
                if in_single_quote {
                    // In single quotes, backslash is literal
                    current_arg.push(c);
                    i += 1;
                } else if in_double_quote {
                    // In double quotes, \\ \" \$ \` are escapes
                    if i + 1 < chars.len() {
                        let next = chars[i + 1];
                        if next == '\\' || next == '"' || next == '$' || next == '`' {
                            current_arg.push(next);
                            i += 2;
                        } else {
                            // Backslash followed by other char is literal
                            current_arg.push(c);
                            i += 1;
                        }
                    } else {
                        // Trailing backslash in quotes - push it
                        current_arg.push(c);
                        i += 1;
                    }
                } else {
                    // Outside quotes, backslash escapes next char
                    if i + 1 < chars.len() {
                        current_arg.push(chars[i + 1]);
                        i += 2;
                    } else {
                        // Trailing backslash produces empty string argument
                        args.push(String::new());
                        i += 1;
                    }
                }
            } else {
                // Windows
                if in_double_quote {
                    // Windows: count consecutive backslashes before quote
                    let mut num_backslashes = 0;
                    let mut j = i;
                    while j < chars.len() && chars[j] == '\\' {
                        num_backslashes += 1;
                        j += 1;
                    }

                    if j < chars.len() && chars[j] == '"' {
                        // Backslashes before quote: 2n → n backslashes + close quote
                        // 2n+1 → n backslashes + literal quote
                        let half = num_backslashes / 2;
                        for _ in 0..half {
                            current_arg.push('\\');
                        }
                        if num_backslashes % 2 == 1 {
                            // Odd number: literal quote
                            current_arg.push('"');
                            i = j + 1;
                        } else {
                            // Even number: close quote
                            in_double_quote = false;
                            i = j + 1;
                        }
                    } else {
                        // Backslashes not before quote - all literal
                        for _ in 0..num_backslashes {
                            current_arg.push('\\');
                        }
                        i = j;
                    }
                } else {
                    // Outside double quotes
                    if i + 1 < chars.len() && chars[i + 1] == '"' {
                        // \" outside quotes is literal quote
                        current_arg.push('"');
                        i += 2;
                    } else {
                        // Backslash is literal
                        current_arg.push(c);
                        i += 1;
                    }
                }
            }
        } else if c == '"' {
            if in_single_quote {
                current_arg.push(c);
                i += 1;
            } else {
                in_double_quote = !in_double_quote;
                i += 1;
            }
        } else if c == '\'' {
            if syntax == CommandLineSyntax::POSIX {
                if in_double_quote {
                    current_arg.push(c);
                    i += 1;
                } else {
                    in_single_quote = !in_single_quote;
                    i += 1;
                }
            } else {
                // Windows: single quotes are literal
                current_arg.push(c);
                i += 1;
            }
        } else if c == ' ' || c == '\t' {
            if in_double_quote || in_single_quote {
                current_arg.push(c);
                i += 1;
            } else {
                // End of argument
                if !current_arg.is_empty()
                    || (syntax == CommandLineSyntax::Windows && args.is_empty())
                {
                    args.push(std::mem::take(&mut current_arg));
                }
                i += 1;
            }
        } else {
            current_arg.push(c);
            i += 1;
        }
    }

    // Don't forget the last argument
    if !current_arg.is_empty() {
        args.push(current_arg);
    } else if syntax == CommandLineSyntax::POSIX {
        // POSIX: trailing backslash outside quotes produces empty string
        // This is handled by the parsing above, but we need to add the empty
        // argument if we ended with just a consumed backslash
    }

    args
}

/// Create a command line string from a vector of arguments.
///
/// Properly escapes/quotes arguments for the given syntax.
pub fn create_command_line(args: &[String], syntax: CommandLineSyntax) -> String {
    let mut result = String::new();

    for (i, arg) in args.iter().enumerate() {
        if i > 0 {
            result.push(' ');
        }

        let needs_quoting = arg.is_empty()
            || arg.chars().any(|c| {
                c == ' '
                    || c == '\t'
                    || c == '"'
                    || c == '\''
                    || c == '\\'
                    || c == '|'
                    || c == '&'
                    || c == ';'
                    || c == '<'
                    || c == '>'
                    || c == '('
                    || c == ')'
                    || c == '$'
                    || c == '`'
                    || c == '!'
                    || c == '#'
                    || c == '*'
                    || c == '?'
                    || c == '['
                    || c == ']'
                    || c == '{'
                    || c == '}'
                    || c == '~'
                    || c == '^'
                    || c == '@'
            });

        if !needs_quoting {
            result.push_str(arg);
        } else if syntax == CommandLineSyntax::POSIX {
            // POSIX: use double quotes, escape \ " $ ` inside
            result.push('"');
            for c in arg.chars() {
                if c == '\\' || c == '"' || c == '$' || c == '`' {
                    result.push('\\');
                }
                result.push(c);
            }
            result.push('"');
        } else {
            // Windows: use double quotes, backslashes before " need doubling
            result.push('"');
            let chars: Vec<char> = arg.chars().collect();
            let mut j = 0;
            while j < chars.len() {
                let c = chars[j];
                if c == '\\' {
                    // Count consecutive backslashes
                    let mut num_backslashes = 0;
                    while j < chars.len() && chars[j] == '\\' {
                        num_backslashes += 1;
                        j += 1;
                    }
                    // If followed by " or end of string, double the backslashes
                    if j == chars.len() || chars[j] == '"' {
                        for _ in 0..num_backslashes * 2 {
                            result.push('\\');
                        }
                        if j < chars.len() && chars[j] == '"' {
                            result.push('\\');
                            result.push('"');
                            j += 1;
                        }
                    } else {
                        // Not followed by quote, backslashes are literal
                        for _ in 0..num_backslashes {
                            result.push('\\');
                        }
                    }
                } else if c == '"' {
                    result.push('\\');
                    result.push('"');
                    j += 1;
                } else {
                    result.push(c);
                    j += 1;
                }
            }
            result.push('"');
        }
    }

    result
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        fn cmd_parse_posix(cmdline: &str) -> Vec<String>;
        fn cmd_parse_windows(cmdline: &str) -> Vec<String>;
        fn cmd_create_posix(args: &[String]) -> String;
        fn cmd_create_windows(args: &[String]) -> String;
    }
}

fn cmd_parse_posix(cmdline: &str) -> Vec<String> {
    parse_command_line(cmdline, CommandLineSyntax::POSIX)
}

fn cmd_parse_windows(cmdline: &str) -> Vec<String> {
    parse_command_line(cmdline, CommandLineSyntax::Windows)
}

fn cmd_create_posix(args: &[String]) -> String {
    create_command_line(args, CommandLineSyntax::POSIX)
}

fn cmd_create_windows(args: &[String]) -> String {
    create_command_line(args, CommandLineSyntax::Windows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posix() -> CommandLineSyntax {
        CommandLineSyntax::POSIX
    }

    fn win32() -> CommandLineSyntax {
        CommandLineSyntax::Windows
    }

    #[test]
    fn test_parse_common() {
        // Common cases that should work the same in both syntaxes
        for syn in [posix(), win32()] {
            assert_eq!(parse_command_line("aa", syn), vec!["aa"]);
            assert_eq!(parse_command_line("a ", syn), vec!["a"]);
            assert_eq!(parse_command_line("\\\" ", syn), vec!["\""]);
            assert_eq!(parse_command_line("\" a\"", syn), vec![" a"]);
            assert_eq!(parse_command_line("\"\\\\\"", syn), vec!["\\"]);
            assert_eq!(parse_command_line("\"\\\"\"", syn), vec!["\""]);
            assert_eq!(parse_command_line("a\" b c\"d ", syn), vec!["a b cd"]);
            assert_eq!(parse_command_line("\\\"a b", syn), vec!["\"a", "b"]);
            assert_eq!(
                parse_command_line("| ! ^ # [", syn),
                vec!["|", "!", "^", "#", "["]
            );
            assert_eq!(
                parse_command_line("; ? * $ &", syn),
                vec![";", "?", "*", "$", "&"]
            );
            assert_eq!(
                parse_command_line(": ` < > ~", syn),
                vec![":", "`", "<", ">", "~"]
            );
        }
    }

    #[test]
    fn test_parse_posix_specific() {
        assert_eq!(parse_command_line(" a", posix()), vec!["a"]);
        assert_eq!(parse_command_line("\\ a", posix()), vec![" a"]);
        assert_eq!(parse_command_line("C:\\ D", posix()), vec!["C: D"]);
        assert_eq!(parse_command_line("C:\\\\ D", posix()), vec!["C:\\", "D"]);
        assert_eq!(parse_command_line("C:\\  D", posix()), vec!["C: ", "D"]);
        assert_eq!(parse_command_line("C:\\\\\\  D", posix()), vec!["C:\\ ", "D"]);
        assert_eq!(parse_command_line("\\", posix()), vec![""]);
        assert_eq!(parse_command_line("\\\\a", posix()), vec!["\\a"]);
        assert_eq!(parse_command_line("\\\\\\a", posix()), vec!["\\a"]);
        assert_eq!(parse_command_line("\\\\", posix()), vec!["\\"]);
        assert_eq!(parse_command_line("\"\\\\a\"", posix()), vec!["\\a"]);
        assert_eq!(parse_command_line("\"\\\\\\a\"", posix()), vec!["\\\\a"]);
        assert_eq!(parse_command_line("'a'' b'", posix()), vec!["a b"]);
        assert_eq!(parse_command_line("'a'", posix()), vec!["a"]);
        assert_eq!(parse_command_line("x' a \\b'", posix()), vec!["x a \\b"]);
    }

    #[test]
    fn test_parse_windows_specific() {
        assert_eq!(parse_command_line(" a", win32()), vec!["", "a"]);
        assert_eq!(parse_command_line("\\ a", win32()), vec!["\\", "a"]);
        assert_eq!(parse_command_line("C:\\ D", win32()), vec!["C:\\", "D"]);
        assert_eq!(parse_command_line("C:\\\\ D", win32()), vec!["C:\\\\", "D"]);
        assert_eq!(parse_command_line("C:\\  D", win32()), vec!["C:\\", "D"]);
        assert_eq!(
            parse_command_line("C:\\\\\\  D", win32()),
            vec!["C:\\\\\\", "D"]
        );
        assert_eq!(parse_command_line("\\", win32()), vec!["\\"]);
        assert_eq!(parse_command_line("\\\\a", win32()), vec!["\\\\a"]);
        assert_eq!(parse_command_line("\\\\\\a", win32()), vec!["\\\\\\a"]);
        assert_eq!(parse_command_line("\\\\", win32()), vec!["\\\\"]);
        assert_eq!(parse_command_line("\"\\\\a\"", win32()), vec!["\\\\a"]);
        assert_eq!(parse_command_line("\"\\\\\\a\"", win32()), vec!["\\\\\\a"]);
        assert_eq!(parse_command_line("'a'' b'", win32()), vec!["'a''", "b'"]);
        assert_eq!(parse_command_line("'a'", win32()), vec!["'a'"]);
        assert_eq!(parse_command_line("x' a \\b'", win32()), vec!["x'", "a", "\\b'"]);
    }

    #[test]
    fn test_create_roundtrip() {
        let test_cases = vec![
            vec!["a".to_string()],
            vec!["a b".to_string()],
            vec!["\"".to_string()],
            vec!["'".to_string()],
            vec!["\\".to_string()],
            vec!["/".to_string()],
            vec!["#".to_string()],
            vec!["$".to_string()],
            vec!["!".to_string()],
            vec!["@".to_string()],
            vec!["`".to_string()],
            vec!["&".to_string()],
            vec!["|".to_string()],
            vec![
                "a\"".to_string(),
                "'x".to_string(),
                "?'\"{".to_string(),
                "])".to_string(),
                "!".to_string(),
                "~`\\".to_string(),
            ],
        ];

        for syn in [posix(), win32()] {
            for arglist in &test_cases {
                let cmdline = create_command_line(arglist, syn);
                let parsed = parse_command_line(&cmdline, syn);
                assert_eq!(&parsed, arglist, "Failed for {:?} with syntax {:?}", arglist, syn);
            }
        }
    }
}
