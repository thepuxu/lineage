# /bugfix Command

Trigger: User types `/bugfix [description]`

## Your Task

Fix a bug efficiently while maintaining quality.

## Process (Lighter than /feature)

1. **CLARIFY** — Understand the bug, classify security impact
2. **IMPLEMENT** — Fix the issue
3. **VERIFY** — Run security scans AND tests, auto-fix issues
4. **DELIVER** — Summary of fix

Skip detailed DESIGN unless the bug is CRITICAL or requires architectural changes.

## Security Classification

Even bugfixes need classification:

- 🔴 CRITICAL: Security vulnerability, auth bypass, data leak
- 🟠 HIGH: Affects user data, breaks access control
- 🟡 STANDARD: Logic error, incorrect behavior
- 🟢 LOW: UI glitch, typo, minor issue

**CRITICAL bugs get full treatment like /feature**

## Regression Prevention

ALWAYS:
- [ ] Add a test that catches this bug
- [ ] Verify related functionality still works
- [ ] Check if bug exists elsewhere (similar code patterns)

## Start

Begin with Phase 1: CLARIFY

Restate the bug and classify its security impact.
