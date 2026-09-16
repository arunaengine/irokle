#!/usr/bin/env python3
"""Token and syntax helpers for the repository style gate."""

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    line: int
    end: int


@dataclass(frozen=True)
class Name:
    kind: str
    text: str
    line: int


@dataclass(frozen=True)
class Comment:
    line: int
    end: int


RUST_WORDS = {
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else",
    "enum", "extern", "false", "fn", "for", "if", "impl", "in", "let", "loop",
    "match", "mod", "move", "mut", "pub", "ref", "return", "self", "Self", "static",
    "struct", "super", "trait", "true", "type", "union", "unsafe", "use", "where",
    "while", "yield",
}


def terms(name):
    """Split normal snake, kebab, and CamelCase names into policy terms."""
    result = []
    for part in re.split(r"[_-]+", name.strip("._-")):
        result.extend(re.findall(r"[A-Z]+(?=[A-Z][a-z]|\d|$)|[A-Z]?[a-z]+|\d+", part))
    return result


def rust_tokens(source):
    """Lex Rust while keeping declarations separate from comments and literals."""
    tokens, comments, errors = [], [], []
    index, line, size = 0, 1, len(source)

    def advance(end):
        nonlocal index, line
        line += source.count("\n", index, end)
        index = end

    while index < size:
        char = source[index]
        if char.isspace():
            advance(index + 1)
            continue
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = size if end < 0 else end
            comments.append(Comment(line, line))
            advance(end)
            continue
        if source.startswith("/*", index):
            start, depth, end = line, 1, index + 2
            while end < size and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
            if depth:
                errors.append((start, "unterminated block comment"))
                end = size
            finish = start + source.count("\n", index, end)
            comments.append(Comment(start, finish))
            advance(end)
            continue
        raw = re.match(r"(?:br|cr|r)(#*)\"", source[index:])
        if raw:
            start = line
            marker = '"' + raw.group(1)
            end = source.find(marker, index + raw.end())
            if end < 0:
                errors.append((start, "unterminated raw string"))
                end = size
            else:
                end += len(marker)
            tokens.append(Token("literal", "", start, start + source.count("\n", index, end)))
            advance(end)
            continue
        prefix = 1 if char in "bc" and index + 1 < size and source[index + 1] in "\"'" else 0
        quote = source[index + prefix] if index + prefix < size else ""
        is_char = quote == "'" and (prefix or bool(re.match(r"'(?:\\.|[^\\'])'", source[index:])))
        if quote == '"' or is_char:
            start, end, escaped = line, index + prefix + 1, False
            while end < size:
                current = source[end]
                end += 1
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == quote:
                    break
            else:
                errors.append((start, "unterminated literal"))
            tokens.append(Token("literal", "", start, start + source.count("\n", index, end)))
            advance(end)
            continue
        match = re.match(r"(?:r#)?[A-Za-z_][A-Za-z0-9_]*", source[index:])
        if match:
            text = match.group(0).removeprefix("r#")
            tokens.append(Token("id", text, line, line))
            advance(index + match.end())
            continue
        if char == "_" or char.isalpha():
            end = index + 1
            while end < size and (source[end] == "_" or source[end].isalnum()):
                end += 1
            tokens.append(Token("id", source[index:end], line, line))
            advance(end)
            continue
        match = re.match(r"(?:\d[\d_]*)(?:\.[\d_]+)?(?:[A-Za-z][A-Za-z0-9_]*)?", source[index:])
        if match:
            tokens.append(Token("number", match.group(0), line, line))
            advance(index + match.end())
            continue
        punct = next((value for value in ("::", "->", "=>", "..=", "...", "..", "&&", "||",
                                                  "<<", ">>", "<=", ">=", "==", "!=", "+=", "-=",
                                                  "*=", "/=", "%=", "&=", "|=", "^=")
                      if source.startswith(value, index)), char)
        tokens.append(Token("punct", punct, line, line))
        advance(index + len(punct))
    linked, _ = pairs(tokens)
    for index, token in enumerate(tokens):
        if token.text != "#":
            continue
        opening = index + 1
        if opening < len(tokens) and tokens[opening].text == "!":
            opening += 1
        closing = linked.get(opening)
        if closing is not None and opening + 3 < closing \
                and tokens[opening + 1].text == "doc" \
                and tokens[opening + 2].text == "=" \
                and tokens[opening + 3].kind == "literal":
            comments.append(Comment(token.line, tokens[closing].end))
    return tokens, comments, errors


def pairs(tokens):
    """Return balanced delimiter partners and syntax errors."""
    opened, result, errors = [], {}, []
    close = {")": "(", "]": "[", "}": "{"}
    for index, token in enumerate(tokens):
        if token.text in ("(", "[", "{"):
            opened.append((token.text, index))
        elif token.text in close:
            if not opened or opened[-1][0] != close[token.text]:
                errors.append((token.line, f"unmatched {token.text}"))
            else:
                _, start = opened.pop()
                result[start] = index
                result[index] = start
    errors.extend((tokens[index].line, f"unclosed {char}") for char, index in opened)
    return result, errors


def nest(stack, text):
    """Track angle brackets only outside expression-containing delimiters."""
    if text in ("(", "[", "{") or text == "<" and all(item == "<" for item in stack):
        stack.append(text)
        return True
    if text in (">", ">>"):
        for _ in text:
            if stack and stack[-1] == "<":
                stack.pop()
        return True
    if text in (")", "]", "}") and stack:
        stack.pop()
        return True
    return False


def segments(tokens, start, end, separator=","):
    """Split a token range at its top-level separator."""
    stack, begin = [], start
    for index in range(start, end):
        text = tokens[index].text
        if not nest(stack, text) and text == separator and not stack:
            yield begin, index
            begin = index + 1
    yield begin, end


def top_find(tokens, start, end, wanted):
    """Find one punctuation token outside nested delimiters."""
    stack = []
    for index in range(start, end):
        text = tokens[index].text
        if not nest(stack, text) and text == wanted and not stack:
            return index
    return end


def pattern_names(tokens, start, end, kind):
    """Extract binding names from a Rust pattern range."""
    result = []
    for index in range(start, end):
        token = tokens[index]
        if token.kind != "id" or token.text == "_" or token.text in RUST_WORDS or token.text[:1].isupper():
            continue
        before = tokens[index - 1].text if index > start else ""
        after = tokens[index + 1].text if index + 1 < end else ""
        if before in (".", "::", "'") or after in ("::", "("):
            continue
        if after == ":" and before != "$":
            continue
        result.append(Name(kind, token.text, token.line))
    return result


def field_names(tokens, start, end):
    """Extract named Rust fields from a declaration body."""
    result = []
    for begin, finish in segments(tokens, start, end):
        stack = []
        for index in range(begin, finish):
            text = tokens[index].text
            if not nest(stack, text) and text == ":" and not stack:
                for prior in range(index - 1, begin - 1, -1):
                    if tokens[prior].kind == "id" and tokens[prior].text not in RUST_WORDS:
                        result.append(Name("field", tokens[prior].text, tokens[prior].line))
                        break
                break
    return result


def angle_end(tokens, start):
    """Find the end of one Rust generic parameter list."""
    depth = 0
    for index in range(start, len(tokens)):
        if tokens[index].text == "<":
            depth += 1
        elif tokens[index].text in (">", ">>"):
            depth -= len(tokens[index].text)
            if depth <= 0:
                return index
    return None


def generic_names(tokens, start):
    """Extract declared lifetime, type, and const generic parameters."""
    if start >= len(tokens) or tokens[start].text != "<":
        return []
    finish = angle_end(tokens, start)
    if finish is None:
        return []
    result = []
    for begin, end in segments(tokens, start + 1, finish):
        found = next((index for index in range(begin, end)
                      if tokens[index].kind == "id" and tokens[index].text not in RUST_WORDS), None)
        if found is not None:
            result.append(Name("parameter", tokens[found].text, tokens[found].line))
    return result


def import_names(tokens):
    """Extract local bindings introduced by Rust use declarations."""
    names = []
    for index, token in enumerate(tokens):
        if token.text != "use" or index + 1 >= len(tokens) or tokens[index + 1].text == "<":
            continue
        finish = index + 1
        while finish < len(tokens) and tokens[finish].text != ";":
            finish += 1
        for pos in range(index + 1, finish):
            current = tokens[pos]
            if current.kind != "id" or current.text == "_" or current.text in RUST_WORDS:
                continue
            before = tokens[pos - 1].text
            after = tokens[pos + 1].text if pos + 1 < finish else ";"
            if before == "as" or after in (",", "}", ";"):
                names.append(Name("binding", current.text, current.line))
    return names


def skip_attrs(tokens, start, end, linked):
    """Skip Rust attributes at the start of one declaration segment."""
    current = start
    while current + 1 < end and tokens[current].text == "#" and tokens[current + 1].text == "[":
        current = linked.get(current + 1, end - 1) + 1
    return current


def rust_names(tokens, macro_inputs=None):
    """Extract repository-owned Rust declarations and bindings."""
    macro_inputs = macro_inputs or {}
    linked, errors = pairs(tokens)
    names, bodies, defined = import_names(tokens), {}, set()
    kinds = {"fn": "function", "struct": "type", "enum": "type", "trait": "trait",
             "type": "type", "union": "type", "const": "constant", "static": "static",
             "mod": "module", "macro": "macro"}
    for index, token in enumerate(tokens):
        if token.text in kinds:
            if token.text == "const" and index + 1 < len(tokens) and tokens[index + 1].text in ("fn", "{"):
                continue
            if token.text == "static" and index and tokens[index - 1].text == "'":
                continue
            following = index + 1
            while following < len(tokens) and tokens[following].text in ("unsafe", "async", "mut"):
                following += 1
            if following < len(tokens) and tokens[following].kind == "id":
                names.append(Name(kinds[token.text], tokens[following].text, tokens[following].line))
                names.extend(generic_names(tokens, following + 1))
                if token.text in ("struct", "enum", "union"):
                    bodies[following] = token.text
        if token.text == "impl":
            names.extend(generic_names(tokens, index + 1))
        if token.text == "macro_rules" and index + 2 < len(tokens) and tokens[index + 1].text == "!":
            if tokens[index + 2].kind == "id":
                names.append(Name("macro", tokens[index + 2].text, tokens[index + 2].line))
                defined.add(tokens[index + 2].text)
        if token.text == "let":
            finish = index + 1
            depth = 0
            while finish < len(tokens):
                text = tokens[finish].text
                if text in ("(", "[", "{"): depth += 1
                elif text in (")", "]", "}"): depth -= 1
                elif depth == 0 and text in ("=", ";", ":"): break
                finish += 1
            names.extend(pattern_names(tokens, index + 1, finish, "variable"))
        if token.text == "for" and index + 1 < len(tokens):
            finish = index + 1
            while finish < len(tokens) and tokens[finish].text not in ("in", "{", ";", "where"):
                finish += 1
            if finish < len(tokens) and tokens[finish].text == "in":
                names.extend(pattern_names(tokens, index + 1, finish, "variable"))
        if token.kind == "id" and token.text in macro_inputs and index + 2 < len(tokens):
            if tokens[index + 1].text == "!" and tokens[index + 2].text in ("(", "[", "{"):
                finish = linked.get(index + 2)
                if finish is not None:
                    rule = macro_inputs[token.text]
                    if rule["mode"] == "single_type":
                        found = next((part for part in range(index + 3, finish)
                                      if tokens[part].kind == "id"), None)
                        if found is not None:
                            names.append(Name("type", tokens[found].text, tokens[found].line))
                    elif rule["mode"] == "function_list":
                        for begin, end in segments(tokens, index + 3, finish, ";"):
                            found = next((part for part in range(begin, end)
                                          if tokens[part].kind == "id"), None)
                            if found is None:
                                continue
                            names.append(Name("function", tokens[found].text, tokens[found].line))
                            opening = next((part for part in range(found + 1, end)
                                            if tokens[part].text == "("), None)
                            if opening in linked:
                                for first, last in segments(tokens, opening + 1, linked[opening]):
                                    colon = top_find(tokens, first, last, ":")
                                    names.extend(pattern_names(tokens, first, colon, "parameter"))
                    else:
                        errors.append((token.line, f"unsupported macro rule: {rule['mode']}"))
    for index, token in enumerate(tokens[:-1]):
        if token.text in defined and tokens[index + 1].text == "!" and token.text not in macro_inputs:
            errors.append((token.line, f"owned macro input is not configured: {token.text}"))
    for index, token in enumerate(tokens):
        if token.text == "fn" and index + 2 < len(tokens):
            opening, angle = None, 0
            for pos in range(index + 2, len(tokens)):
                if tokens[pos].text == "<":
                    angle += 1
                elif tokens[pos].text in (">", ">>") and angle:
                    angle = max(0, angle - len(tokens[pos].text))
                elif tokens[pos].text == "(" and angle == 0:
                    opening = pos
                    break
                elif tokens[pos].text in (";", "{") and angle == 0:
                    break
            if opening is not None and opening in linked:
                for begin, end in segments(tokens, opening + 1, linked[opening]):
                    begin = skip_attrs(tokens, begin, end, linked)
                    colon = top_find(tokens, begin, end, ":")
                    names.extend(pattern_names(tokens, begin, colon, "parameter"))
        if token.text == "|" and index + 1 < len(tokens):
            before = tokens[index - 1].text if index else ""
            if before not in ("=", "(", "[", "{", ",", "move", "async", "return", "=>"):
                continue
            finish = next((pos for pos in range(index + 1, len(tokens))
                           if tokens[pos].text in ("|", ";", "{")), None)
            if finish is not None and tokens[finish].text == "|":
                for begin, end in segments(tokens, index + 1, finish):
                    begin = skip_attrs(tokens, begin, end, linked)
                    colon = top_find(tokens, begin, end, ":")
                    names.extend(pattern_names(tokens, begin, colon, "parameter"))
    for index, token in enumerate(tokens):
        if token.text != "match":
            continue
        opening = index + 1
        while opening < len(tokens):
            if tokens[opening].text == "{" and opening in linked:
                closing = linked[opening]
                has_arm = any(tokens[pos].text == "=>" for pos in range(opening + 1, closing))
                if has_arm:
                    for begin, end in segments(tokens, opening + 1, closing):
                        arrow = next((pos for pos in range(begin, end)
                                      if tokens[pos].text == "=>"), None)
                        if arrow is not None:
                            names.extend(pattern_names(tokens, begin, arrow, "variable"))
                    break
                opening = closing
            if opening >= len(tokens) or tokens[opening].text == ";":
                break
            opening += 1
    for name_index, kind in bodies.items():
        start = name_index + 1
        if start < len(tokens) and tokens[start].text == "<":
            end = angle_end(tokens, start)
            if end is None or any(token.text == "{" for token in tokens[start:end]):
                errors.append((tokens[start].line, "generic blocks require an explicit parser rule"))
                continue
            start = end + 1
        opening = next((pos for pos in range(start, len(tokens))
                        if tokens[pos].text in ("{", ";")), None)
        if opening is None or tokens[opening].text != "{" or opening not in linked:
            continue
        closing = linked[opening]
        if kind in ("struct", "union"):
            names.extend(field_names(tokens, opening + 1, closing))
        else:
            for begin, end in segments(tokens, opening + 1, closing):
                begin = skip_attrs(tokens, begin, end, linked)
                variant = next((pos for pos in range(begin, end)
                                if tokens[pos].kind == "id" and tokens[pos].text not in RUST_WORDS), None)
                if variant is None:
                    continue
                names.append(Name("variant", tokens[variant].text, tokens[variant].line))
                brace = next((pos for pos in range(variant + 1, end) if tokens[pos].text == "{"), None)
                if brace is not None and brace in linked:
                    names.extend(field_names(tokens, brace + 1, linked[brace]))
    return names, errors


def requires_test(tokens, start, end):
    """Recognize cfg predicates that cannot enable a production import."""
    if end == start + 1:
        return tokens[start].text == "test"
    if end < start + 3 or tokens[start + 1].text != "(" or tokens[end - 1].text != ")":
        return False
    children = [requires_test(tokens, begin, finish)
                for begin, finish in segments(tokens, start + 2, end - 1)
                if begin < finish]
    if tokens[start].text == "all":
        return any(children)
    if tokens[start].text == "any":
        return bool(children) and all(children)
    return False


def test_ranges(tokens):
    """Locate modules and imports with a directly attached test-only cfg."""
    linked, _ = pairs(tokens)
    ranges = []
    for index, token in enumerate(tokens):
        if token.text != "#" or index + 4 >= len(tokens):
            continue
        if [item.text for item in tokens[index + 1:index + 4]] != ["[", "cfg", "("]:
            continue
        closing = linked.get(index + 3)
        if closing is None or not requires_test(tokens, index + 4, closing):
            continue
        target = skip_attrs(tokens, linked[index + 1] + 1, len(tokens), linked)
        if target < len(tokens) and tokens[target].text == "pub":
            target += 1
            if target < len(tokens) and tokens[target].text == "(":
                target = linked[target] + 1
        if target >= len(tokens) or tokens[target].text not in ("mod", "use"):
            continue
        opening = next((pos for pos in range(target + 1, len(tokens))
                        if tokens[pos].text in ("{", ";")), None)
        if opening is not None:
            end = linked[opening] if tokens[opening].text == "{" else opening
            ranges.append((target - 1, end))
    return ranges


def rust_wildcards(tokens):
    """Return production wildcard-import lines."""
    ranges = test_ranges(tokens)
    result = []
    for index, token in enumerate(tokens):
        if token.text != "use" or index + 1 >= len(tokens) or tokens[index + 1].text == "<":
            continue
        finish = index + 1
        while finish < len(tokens) and tokens[finish].text != ";":
            finish += 1
        wildcard = any(tokens[pos].text == "*" for pos in range(index + 1, finish))
        guarded = any(start < index < end for start, end in ranges)
        if wildcard and not guarded:
            result.append(token.line)
    return result
