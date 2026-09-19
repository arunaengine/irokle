#!/usr/bin/env python3
"""Check repository-owned files against the canonical root STYLE.md."""

import argparse
import ast
from dataclasses import dataclass
import io
import json
import keyword
from pathlib import Path
import re
import subprocess
import sys
import tokenize

from style_tokens import Comment, Name, Scan, rust_names, rust_tokens, rust_wildcards, terms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--rules", type=Path, default=Path(__file__).with_name("style_rules.json"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        root = args.root.resolve(strict=True)
        issues = check(root, load_rules(args.rules.resolve(strict=True)))
    except (OSError, ValueError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"style check failed: {error}", file=sys.stderr)
        return 2
    manual = ["domain ownership, prose quality, and meaningful term boundaries", "control-flow reading order",
              "external-contract evidence and compatibility"]
    if args.json:
        print(json.dumps({"issues": [issue.__dict__ for issue in issues],
                          "manual_review": manual}, indent=2))
    else:
        for issue in issues:
            print(f"{issue.path}:{issue.line}: {issue.rule}: {issue.name}: {issue.detail}")
        print("manual review remains required: " + "; ".join(manual), file=sys.stderr)
    return int(bool(issues))



def check(root, rules):
    """Return all automated policy violations for one repository tree."""
    files = owned_files(root)
    paths = [file.relative_to(root).as_posix() for file in files]
    issues = layout_issues(paths, rules)
    for file in files:
        issues.extend(file_issues(root, file, rules))
    return apply_exceptions(issues, rules)


def load_rules(path):
    """Load and validate the reviewable policy data."""
    rules = json.loads(path.read_text())
    needed = {"maintained_extensions", "source_extensions", "required_roots", "small_domains",
              "generic_prefixes", "skip_paths", "exceptions", "macro_inputs", "test_paths"}
    missing = needed - rules.keys()
    if missing:
        raise ValueError(f"missing rule keys: {sorted(missing)}")
    for entry in [*rules["small_domains"], *rules["skip_paths"], *rules["exceptions"]]:
        if not entry.get("reason") or not entry.get("source"):
            raise ValueError(f"exception lacks reason or source: {entry}")
    for entry in rules["small_domains"]:
        if len(entry.get("members", [])) < 3 or len(entry["members"]) != len(set(entry["members"])):
            raise ValueError(f"small domain needs at least three exact members: {entry}")
    for entry in rules["exceptions"]:
        if not {"path", "rule", "name", "kind"} <= entry.keys():
            raise ValueError(f"symbol exception is not exact: {entry}")
    for entry in rules["skip_paths"]:
        if ("path" in entry) == ("prefix" in entry) or not entry.get("rules"):
            raise ValueError(f"path exception is not narrow: {entry}")
        if not set(entry["rules"]) <= {"content", "layout"}:
            raise ValueError(f"unknown path exception rule: {entry}")
    modes = {entry.get("mode") for entry in rules["macro_inputs"].values()}
    if not modes <= {"single_type", "function_list"}:
        raise ValueError(f"unsupported macro modes: {sorted(modes)}")
    return rules



@dataclass(frozen=True, order=True)
class Issue:
    path: str
    line: int
    rule: str
    name: str
    kind: str
    detail: str


class PythonNames(ast.NodeVisitor):
    """Collect Python definitions, bindings, parameters, and assigned fields."""

    def __init__(self):
        self.names = []

    def add(self, kind, name, line):
        if name != "_" and not keyword.iskeyword(name):
            self.names.append(Name(kind, name, line))

    def visit_FunctionDef(self, node):
        self.add("function", node.name, node.lineno)
        self.args(node.args)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self.visit_FunctionDef(node)

    def visit_Lambda(self, node):
        self.args(node.args)
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        self.add("type", node.name, node.lineno)
        self.generic_visit(node)

    def visit_Name(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Param)):
            self.add("variable", node.id, node.lineno)

    def visit_Attribute(self, node):
        if isinstance(node.ctx, ast.Store):
            self.add("field", node.attr, node.lineno)
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            self.add("variable", alias.asname or alias.name.split(".")[0], node.lineno)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            if alias.name != "*":
                self.add("variable", alias.asname or alias.name, node.lineno)

    def visit_ExceptHandler(self, node):
        if node.name:
            self.add("variable", node.name, node.lineno)
        self.generic_visit(node)

    def visit_MatchAs(self, node):
        if node.name:
            self.add("variable", node.name, node.lineno)
        self.generic_visit(node)

    def visit_MatchStar(self, node):
        if node.name:
            self.add("variable", node.name, node.lineno)

    def args(self, arguments):
        values = [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs]
        if arguments.vararg:
            values.append(arguments.vararg)
        if arguments.kwarg:
            values.append(arguments.kwarg)
        for value in values:
            self.add("parameter", value.arg, value.lineno)


def python_scan(source):
    """Parse Python names, comments, docstrings, and wildcard imports."""
    try:
        tree = ast.parse(source)
    except SyntaxError as error:
        return Scan([], [], [], [(error.lineno or 1, error.msg)])
    visitor = PythonNames()
    visitor.visit(tree)
    comments = []
    try:
        stream = tokenize.generate_tokens(io.StringIO(source).readline)
        comments.extend(Comment(item.start[0], item.end[0]) for item in stream
                        if item.type == tokenize.COMMENT
                        and not (item.start == (1, 0) and item.string.startswith("#!")))
    except tokenize.TokenError as error:
        return Scan(visitor.names, comments, [], [(error.args[1][0], error.args[0])])
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                comments.append(Comment(body[0].lineno, body[0].end_lineno or body[0].lineno))
    wildcards = [node.lineno for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                 and any(alias.name == "*" for alias in node.names)]
    return Scan(visitor.names, comments, wildcards, [])


def shell_substitutions(source):
    """Extract active simple command substitutions without reading quoted prose."""
    bodies, errors, index, quote = [], [], 0, None
    while index < len(source):
        char = source[index]
        if char == "\\" and quote != "'":
            index += 2
            continue
        if quote == "'":
            quote = None if char == "'" else quote
            index += 1
            continue
        if char == "'" and quote is None:
            quote, index = "'", index + 1
            continue
        if char == '"':
            quote = None if quote == '"' else '"'
            index += 1
            continue
        if char == "#" and quote is None:
            end = source.find("\n", index)
            index = len(source) if end < 0 else end + 1
            continue
        if source.startswith("$((", index):
            errors.append((source[:index].count("\n") + 1, "unsupported shell arithmetic syntax"))
            index += 3
            continue
        if source.startswith("$(", index):
            end = source.find(")", index + 2)
            body = source[index + 2:end] if end >= 0 else ""
            if end < 0 or "\n" in body or "$(" in body or "(" in body:
                errors.append((source[:index].count("\n") + 1,
                               "nested or multiline command substitution is unsupported"))
                index += 2
                continue
            bodies.append((source[:index].count("\n") + 1, body))
            index = end + 1
            continue
        index += 1
    return bodies, errors


def shell_scan(source):
    """Inspect declared names in the supported shell subset."""
    names, comments, errors, clean = [], [], [], []
    substitutions, substitution_errors = shell_substitutions(source)
    errors.extend(substitution_errors)
    for number, line in enumerate(source.splitlines(), 1):
        quote, escaped, output = None, False, []
        for char in line:
            if escaped:
                escaped = False
                output.append(" ")
            elif char == "\\" and quote != "'":
                escaped = True
                output.append(" ")
            elif quote:
                if char == quote:
                    quote = None
                output.append(" ")
            elif char in "'\"":
                quote = char
                output.append(" ")
            elif char == "#":
                comments.append(Comment(number, number))
                output.extend(" " * (len(line) - len(output)))
                break
            else:
                output.append(char)
        if quote:
            errors.append((number, "multiline shell quotes are unsupported"))
        clean.append("".join(output))
    text = "\n".join(clean)
    unsupported = ((r"(^|[^<])<<<?", "redirection"),
                   (r"(^\s*|[;&|]\s*)(case|select|coproc|eval)\b", "command"),
                   (r"[<>]\(", "process substitution"))
    for pattern, label in unsupported:
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            errors.append((text[:match.start()].count("\n") + 1,
                           f"unsupported shell {label} syntax"))
    for line, body in substitutions:
        if re.search(r"\b(?:local|declare|typeset|read|export|readonly|function)\b|\b\w+\s*=", body):
            errors.append((line, "declarations in command substitution are unsupported"))
    for number, line in enumerate(clean, 1):
        for match in re.finditer(r"(?:^|[\s;(&|])([A-Za-z_][A-Za-z0-9_]*)(?:\[[^]]+\])?\s*=", line):
            names.append(Name("variable", match.group(1), number))
        for match in re.finditer(r"(?:^\s*|[;&]\s*)(?:function\s+)?([A-Za-z_][A-Za-z0-9_-]*)\s*\(\s*\)", line):
            names.append(Name("function", match.group(1), number))
        for match in re.finditer(r"(?:^\s*|[;&]\s*)function\s+([A-Za-z_][A-Za-z0-9_-]*)\s*(?:\{|$)", line):
            names.append(Name("function", match.group(1), number))
        match = re.search(r"\bfor\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\b", line)
        if match:
            names.append(Name("variable", match.group(1), number))
        for match in re.finditer(r"\b(?:local|declare|typeset|read|export|readonly)\b([^;&|]*)", line):
            for word in match.group(1).split():
                if word.startswith("-"):
                    continue
                found = re.match(r"([A-Za-z_][A-Za-z0-9_]*)(?:\[[^]]+\])?(?:=|$)", word)
                if found:
                    names.append(Name("variable", found.group(1), number))
    return Scan(names, comments, [], errors)


def owned_files(root):
    """List tracked and visible maintained work without following symlinks."""
    if (root / ".git").exists():
        command = ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others",
                   "--exclude-standard"]
        result = subprocess.run(command, check=True, capture_output=True)
        names = sorted(set(result.stdout.split(b"\0")) - {b""})
        files = [root / name.decode(errors="surrogateescape") for name in names]
        return [path for path in files if path.is_file() or path.is_symlink()]
    return sorted(path for path in root.rglob("*") if path.is_file() and ".git" not in path.parts)


def path_match(path, entry):
    """Match an exact path or an explicitly declared subtree."""
    if "path" in entry:
        return path == entry["path"]
    prefix = entry["prefix"].rstrip("/") + "/"
    return path.startswith(prefix)


def skipped(path, rules, category):
    """Apply only skip entries naming the current check category."""
    return any(category in entry["rules"] and path_match(path, entry)
               for entry in rules["skip_paths"])


def test_path(path, rules):
    """Recognize files whose wildcard imports are private test implementation."""
    return any(
        path == value or path.startswith(value.rstrip("/") + "/")
        for value in rules["test_paths"])


def comment_issues(path, comments, source):
    """Reject logical comment blocks with more than three physical lines."""
    if not comments:
        return []
    ordered = sorted(set(comments), key=lambda item: (item.line, item.end))
    blocks = []
    lines = source.splitlines()
    start, end, count = ordered[0].line, ordered[0].end, ordered[0].end - ordered[0].line + 1
    for comment in ordered[1:]:
        gap = lines[end:comment.line - 1]
        if comment.line <= end + 1 or all(not line.strip() for line in gap):
            count += max(0, comment.end - max(end, comment.line) + 1)
            end = max(end, comment.end)
        else:
            blocks.append((start, end, count))
            start, end = comment.line, comment.end
            count = comment.end - comment.line + 1
    blocks.append((start, end, count))
    return [Issue(path, start, "comment_lines", "comment", "comment", f"logical comment uses {count} source lines")
            for start, _, count in blocks if count > 3]


def file_issues(root, file, rules):
    """Check syntax-aware names, comments, and imports in one source file."""
    path = file.relative_to(root).as_posix()
    suffix = file.suffix.lower()
    if suffix not in rules["source_extensions"] or skipped(path, rules, "content"):
        return []
    if file.is_symlink():
        return [Issue(path, 1, "syntax", "symlink", "file",
                      "maintained source symlink needs an exact exception")]
    try:
        source = file.read_text()
    except (OSError, UnicodeError) as error:
        return [Issue(path, 1, "syntax", "file", "file", str(error))]
    names, comments, wildcards, errors = [], [], [], []
    if suffix == ".rs":
        tokens, comments, errors = rust_tokens(source)
        found, name_errors = rust_names(tokens, rules["macro_inputs"])
        names, errors = found, [*errors, *name_errors]
        wildcards = rust_wildcards(tokens)
    elif suffix == ".py":
        scan = python_scan(source)
        names, comments, wildcards, errors = scan.names, scan.comments, scan.wildcards, scan.errors
    elif suffix == ".sh":
        syntax = subprocess.run(["bash", "-n", str(file)], capture_output=True, text=True)
        scan = shell_scan(source)
        names, comments, wildcards, errors = scan.names, scan.comments, scan.wildcards, scan.errors
        if syntax.returncode:
            errors.append((1, syntax.stderr.strip() or "bash rejected source"))
    issues = [Issue(path, line, "syntax", "source", "source", detail) for line, detail in errors]
    seen = set()
    for name in names:
        if (name.line, name.text) in seen:
            continue
        seen.add((name.line, name.text))
        if not name.text.isascii() or not terms(name.text):
            issues.append(Issue(path, name.line, "name_syntax", name.text, name.kind,
                                f"{name.kind} needs reviewed term boundaries"))
        elif len(terms(name.text)) > 3:
            issues.append(Issue(path, name.line, "name_terms", name.text, name.kind,
                                f"{name.kind} has {len(terms(name.text))} terms"))
    if not test_path(path, rules):
        issues.extend(Issue(path, line, "wildcard_import", "*", "import", "production import must be explicit")
                      for line in wildcards)
    lines = source.splitlines()
    notices = set(rules.get("mandatory_notices", []))
    comments = [comment for comment in comments
                if comment.line != comment.end or lines[comment.line - 1].strip() not in notices]
    issues.extend(comment_issues(path, comments, source))
    return issues


def stem_name(path):
    """Return the policy stem for maintained source and asset files."""
    name = Path(path).name
    if name.startswith(".") and name.count(".") == 1:
        return name[1:]
    return Path(name).stem


def layout_issues(paths, rules):
    """Check filename terms, shared prefixes, and optional folder size."""
    extensions = set(rules["maintained_extensions"])
    visible = [path for path in paths if not skipped(path, rules, "layout")]
    maintained = [path for path in visible if Path(path).suffix.lower() in extensions]
    issues = [Issue(path, 1, "unsupported_asset", Path(path).name, "asset",
                    "owned file type is not classified by style rules")
              for path in visible if Path(path).suffix.lower() not in extensions]
    for path in maintained:
        stem = stem_name(path)
        if stem != "mod" and (not stem.isascii() or not terms(stem)):
            issues.append(Issue(path, 1, "name_syntax", stem, "file",
                                "source filename needs reviewed term boundaries"))
        elif stem != "mod" and len(terms(stem)) > 3:
            issues.append(Issue(path, 1, "file_terms", stem, "file",
                                f"source filename has {len(terms(stem))} terms"))
    folders = {}
    for path in maintained:
        parent = Path(path).parent.as_posix()
        folders.setdefault("." if parent == "." else parent, []).append(path)
        for ancestor in Path(path).parents:
            folders.setdefault(ancestor.as_posix(), [])
    required = set(rules["required_roots"])
    domains = {entry["path"]: entry for entry in rules["small_domains"]}
    generic = set(rules["generic_prefixes"])
    for folder, members in sorted(folders.items()):
        direct = [path for path in members if stem_name(path) != "mod"]
        domain = domains.get(folder)
        valid_domain = domain is not None and {Path(path).name for path in direct} == set(domain["members"])
        if domain is not None and not valid_domain:
            issues.append(Issue(folder, 1, "domain_members", Path(folder).name, "folder",
                                "small domain does not match its reviewed member set"))
        prefixes = {}
        for path in direct:
            stem = stem_name(path)
            if "_" in stem or "-" in stem:
                prefix = re.split(r"[_-]", stem, maxsplit=1)[0]
                if prefix not in generic:
                    prefixes.setdefault(prefix, []).append(path)
        for prefix, grouped in sorted(prefixes.items()):
            if len(grouped) >= 3:
                issues.append(Issue(folder, 1, "shared_prefix", prefix, "folder",
                                    f"{len(grouped)} sibling files require a {prefix} domain"))
        if folder not in required and not valid_domain and len(direct) < 5:
            issues.append(Issue(folder, 1, "folder_size", Path(folder).name, "folder",
                                f"optional folder has {len(direct)} direct maintained files"))
    for folder in domains.keys() - folders.keys():
        issues.append(Issue(folder, 1, "domain_members", Path(folder).name, "folder",
                            "reviewed small domain is absent"))
    return issues


def apply_exceptions(issues, rules):
    """Remove exact reviewed exceptions and report stale entries."""
    entries = rules["exceptions"]
    used = set()
    kept = []
    for issue in issues:
        match = next((index for index, entry in enumerate(entries)
                      if issue.path == entry["path"] and issue.rule == entry["rule"]
                      and issue.name == entry["name"] and issue.kind == entry["kind"]), None)
        if match is None:
            kept.append(issue)
        else:
            used.add(match)
    for index, entry in enumerate(entries):
        if index not in used:
            kept.append(Issue(entry["path"], 1, "stale_exception", entry["name"], entry["kind"],
                              "reviewed exception does not match a current violation"))
    return sorted(set(kept))




if __name__ == "__main__":
    sys.exit(main())
