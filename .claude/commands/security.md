# /security Command

Trigger: User types `/security [optional: specific file or directory]`

## Your Task

Comprehensive security review of the codebase (or specified path).

## Process

### 1. Run Automated Tools

Execute all available security tools:

**Python projects:**
```bash
bandit -r . -f json 2>/dev/null || echo "bandit: not installed"
pip-audit 2>/dev/null || echo "pip-audit: not installed"  
safety check 2>/dev/null || echo "safety: not installed"
```

**JavaScript/TypeScript projects:**
```bash
npm audit 2>/dev/null || echo "npm audit: failed or not npm project"
```

**Any project:**
```bash
# Secrets in code
echo "=== Checking for hardcoded secrets ==="
grep -rn "password\s*=\s*['\"][^'\"]\{8,\}['\"]" --include="*.py" --include="*.js" --include="*.ts" --include="*.go" . 2>/dev/null | grep -v node_modules | grep -v __pycache__ | grep -v ".git" || echo "No password patterns found"

grep -rn "api[_-]\?key\s*=\s*['\"][^'\"]\{16,\}['\"]" --include="*.py" --include="*.js" --include="*.ts" --include="*.go" . 2>/dev/null | grep -v node_modules | grep -v __pycache__ | grep -v ".git" || echo "No API key patterns found"

grep -rn "secret\s*=\s*['\"][^'\"]\{8,\}['\"]" --include="*.py" --include="*.js" --include="*.ts" --include="*.go" . 2>/dev/null | grep -v node_modules | grep -v __pycache__ | grep -v ".git" || echo "No secret patterns found"

grep -rn "AKIA[A-Z0-9]\{16\}" . 2>/dev/null | grep -v node_modules | grep -v ".git" || echo "No AWS keys found"

grep -rn "-----BEGIN.*PRIVATE KEY-----" . 2>/dev/null | grep -v node_modules | grep -v ".git" || echo "No private keys found"
```

### 2. Manual Code Review

Check for:

**Authentication & Authorization**
- [ ] Auth required on all protected routes
- [ ] Session handling is secure
- [ ] Password hashing uses bcrypt/argon2 (not MD5/SHA1)
- [ ] No hardcoded credentials

**Input Validation**
- [ ] All user inputs validated
- [ ] SQL queries parameterized (no string concatenation)
- [ ] File uploads validated (type, size, name)
- [ ] No command injection vectors

**Data Protection**
- [ ] Sensitive data not logged
- [ ] PII encrypted at rest (if applicable)
- [ ] HTTPS enforced (if web)
- [ ] CORS configured properly

**Dependencies**
- [ ] No known vulnerable dependencies
- [ ] Dependencies from trusted sources

### 3. Report Findings

Format:

```markdown
## 🔒 Security Review

### Scan Results
| Tool | Status | Findings |
|------|--------|----------|
| [tool] | ✅/❌ | [count] |

### Critical Issues 🔴
[List any critical issues - must fix]

### High Issues 🟠  
[List high priority issues - should fix]

### Medium Issues 🟡
[List medium issues - consider fixing]

### Low Issues 🟢
[List low priority issues - nice to fix]

### Manual Review Notes
[Anything concerning from code review]

### Recommendations
1. [Top recommendation]
2. [Second recommendation]
...

### Tools Not Available
[List tools that weren't installed]
```

### 4. Offer to Fix

After reporting:

"Would you like me to fix any of these issues? I can address:
- [List fixable issues]

Say 'fix all', 'fix critical', or specify which issues."

## Start

Begin scanning the codebase (or specified path).
