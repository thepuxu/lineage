# /qa Command

Trigger: User types `/qa [optional: specific file or directory]`

## Your Task

Comprehensive quality assurance review.

## Process

### 1. Run Tests

Detect and run the project's test framework:

**Python:**
```bash
pytest -v --tb=short 2>/dev/null || python -m pytest -v --tb=short 2>/dev/null || python -m unittest discover -v 2>/dev/null || echo "No Python tests found"
```

**JavaScript/TypeScript:**
```bash
npm test 2>/dev/null || yarn test 2>/dev/null || echo "No JS tests configured"
```

**Go:**
```bash
go test ./... -v 2>/dev/null || echo "No Go tests found"
```

### 2. Check Coverage (if available)

**Python:**
```bash
pytest --cov=. --cov-report=term-missing 2>/dev/null || echo "Coverage not available"
```

**JavaScript:**
```bash
npm test -- --coverage 2>/dev/null || echo "Coverage not available"
```

### 3. Code Quality Checks

**Linting:**
```bash
# Python
ruff check . 2>/dev/null || flake8 . 2>/dev/null || pylint **/*.py 2>/dev/null || echo "No Python linter"

# JavaScript/TypeScript  
npx eslint . 2>/dev/null || echo "No ESLint"

# Go
go vet ./... 2>/dev/null || echo "No Go vet"
```

**Type Checking:**
```bash
# Python
mypy . 2>/dev/null || echo "No mypy"

# TypeScript
npx tsc --noEmit 2>/dev/null || echo "No TypeScript checking"
```

### 4. Identify Test Gaps

Review code and identify:
- Functions without tests
- Edge cases not covered
- Error paths not tested
- Integration points not tested

### 5. Report

```markdown
## 🧪 QA Report

### Test Results
- **Status**: ✅ PASS / ❌ FAIL
- **Tests Run**: [count]
- **Passed**: [count]
- **Failed**: [count]
- **Skipped**: [count]

### Coverage
- **Overall**: [percentage or "not measured"]
- **Uncovered Areas**:
  - [file/function]: [reason]

### Code Quality
| Check | Status | Issues |
|-------|--------|--------|
| Linting | ✅/❌ | [count] |
| Type Check | ✅/❌ | [count] |

### Test Gaps Identified
1. [Function/area without tests]
2. [Edge case not covered]
...

### Recommendations
1. [Most important test to add]
2. [Second priority]
...
```

### 6. Offer to Improve

After reporting:

"Would you like me to:
- Fix failing tests
- Add tests for uncovered functions
- Fix linting issues

Specify what you'd like me to address."

## Start

Begin running tests and quality checks.
