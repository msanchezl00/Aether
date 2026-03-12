# Project Style Guide (Google-Inspired)

> **Goal:** establish a shared, clear, and maintainable baseline for all code in this repository.
>
> This guide is directly based on Google Style Guides and Google Engineering Practices, adapted to this project's stack.

---

## 1) Official References (Google)

### 1.1 Google Style Guides index
- https://google.github.io/styleguide/

### 1.2 Language / technology guides
- Python Style Guide: https://google.github.io/styleguide/pyguide.html
- JavaScript Style Guide: https://google.github.io/styleguide/jsguide.html
- HTML/CSS Style Guide: https://google.github.io/styleguide/htmlcssguide.html
- C++ Style Guide: https://google.github.io/styleguide/cppguide.html
- Shell Style Guide: https://google.github.io/styleguide/shellguide.html
- Go Style Decisions (Google): https://google.github.io/styleguide/go/decisions.html
- Go Best Practices (Google): https://google.github.io/styleguide/go/best-practices.html
- TypeScript Guide (Google): https://google.github.io/styleguide/tsguide.html

### 1.3 Engineering and code review practices
- Engineering Practices (overview): https://google.github.io/eng-practices/
- Code Review Developer Guide: https://google.github.io/eng-practices/review/
- Google Developer Documentation Style Guide: https://developers.google.com/style

> If personal preferences conflict with these references, prioritize **team and repository consistency**.

---

## 2) Guiding Principles

1. **Readability first**
   - Code is written for humans first.
   - Prefer explicit names, straightforward flow, and low cognitive load.

2. **Consistency over personal preference**
   - Follow established repository patterns.
   - Avoid author-specific micro-styles.

3. **Simplicity and focus**
   - Keep functions/modules small and single-purpose.
   - Minimize branching, coupling, and side effects.

4. **Maintainability by design**
   - Use clear interfaces and explicit boundaries.
   - Keep changes easy to understand, test, and revert.

5. **Explicit errors and observability**
   - Do not hide failures.
   - Log actionable context (without leaking secrets).

6. **Secure by default**
   - Treat external input as untrusted.
   - Validate, sanitize, and apply least privilege.

7. **Testing as part of design**
   - Design code so it is naturally testable.
   - Prefer deterministic, high-value tests.

8. **Performance based on evidence**
   - Measure before optimizing.
   - Avoid premature complexity.

9. **Stable and predictable APIs**
   - Prefer backward-compatible changes.
   - When breaking changes are required, version and document them.

10. **Shared quality ownership**
    - Reviews should improve code quality, not just approve it.
    - Everyone shares responsibility for technical quality.

---

## 3) General Coding Conventions

### 3.1 Naming
- Variables, functions, and modules must communicate intent.
- Avoid unclear abbreviations (`tmp`, `obj`, `data2`) except in trivial scopes.
- Prefer domain-language names where relevant.

### 3.2 Functions and methods
- Functions should do one thing well.
- Avoid long functions with mixed responsibilities.
- Limit parameter count; prefer structs/objects when complexity grows.

### 3.3 Comments and documentation
- Document **why**, not the obvious **what**.
- Keep comments aligned with actual behavior.
- Add context for non-trivial decisions.

### 3.4 Error handling
- Handle errors at the right level with actionable messages.
- Avoid silent failures and generic catches without context.
- Preserve root-cause information when possible.

### 3.5 Configuration and secrets
- Never hardcode credentials, tokens, or sensitive endpoints.
- Load configuration from environment variables or non-secret versioned files.
- Document required variables clearly.

### 3.6 Dependencies
- Keep dependencies minimal and justified.
- Avoid adding libraries for simple problems.
- Consider maintenance status, licensing, and security impact.

---

## 4) Repository Stack Guidance

This repository contains components in **Go**, **Python**, and **JavaScript/TypeScript**.

### 4.1 Go
- Use standard formatting (`gofmt`) and idiomatic Go conventions.
- Prefer composition over complex hierarchies.
- Return explicit errors with context.
- Main references:
  - https://google.github.io/styleguide/go/decisions.html
  - https://google.github.io/styleguide/go/best-practices.html

### 4.2 Python
- Follow Google Python Style Guide conventions for structure and clarity.
- Prefer small functions, useful docstrings, and type hints where valuable.
- Main reference:
  - https://google.github.io/styleguide/pyguide.html

### 4.3 JavaScript / TypeScript
- Keep module boundaries, imports, and naming consistent.
- Avoid accidental complexity in UI/API logic.
- Main references:
  - https://google.github.io/styleguide/jsguide.html
  - https://google.github.io/styleguide/tsguide.html

### 4.4 Shell scripts
- Write robust scripts with explicit error handling, quoting, and argument validation.
- Main reference:
  - https://google.github.io/styleguide/shellguide.html

### 4.5 Documentation and Markdown
- Document technical decisions and operational flows.
- Use clear headings and runnable examples when possible.
- Editorial style reference:
  - https://developers.google.com/style

---

## 5) Testing and Quality

### 5.1 Minimum rules
- Every change must validate expected behavior.
- Bug fixes should add or update tests when feasible.
- Avoid brittle tests tightly coupled to irrelevant internals.

### 5.2 What to test
- Happy paths and edge cases.
- Error handling and input validation.
- Integration between critical modules.

### 5.3 Acceptance criteria
- If automated testing is not possible, explain validation steps in the PR.
- Validation must be reproducible by another team member.

---

## 6) Pull Requests and Reviews

### 6.1 PR requirements
- Clear, descriptive title.
- Problem statement and solution summary.
- Scoped changes (avoid mixing major refactors with feature changes).
- Validation evidence (tests, commands, screenshots when applicable).

### 6.2 Review best practices (Google-inspired)
- Review for correctness, design, readability, tests, and maintainability.
- Give specific, actionable feedback.
- Distinguish blocking issues from suggestions.
- Keep communication professional and collaborative.

Reference:
- https://google.github.io/eng-practices/review/

---

## 7) Quick Pre-Merge Checklist

- [ ] Change is readable and consistent with existing code.
- [ ] No secrets or insecure configuration introduced.
- [ ] Errors are handled with sufficient context.
- [ ] Validation is included (tests/commands) or explicitly justified.
- [ ] Documentation is updated for any public behavior change.
- [ ] PR clearly explains what changed and why.

---

## 8) Tie-Breaker Rule

When multiple valid options exist:
1. Choose the most readable option.
2. Choose the option most consistent with the repository.
3. Choose the option with the lowest long-term maintenance cost.

If uncertainty remains, follow the relevant Google reference for the language being changed.
