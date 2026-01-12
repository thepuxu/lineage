# Option A: Enhanced Claude Code SDLC

This is a structured development workflow using Claude Code's native features.

## What's Included

```
your-project/
├── CLAUDE.md                      # Master instructions (auto-loaded)
└── .claude/
    └── commands/
        ├── feature.md             # /feature - full SDLC
        ├── bugfix.md              # /bugfix - lighter process
        ├── security.md            # /security - security review
        ├── qa.md                  # /qa - quality assurance
        └── review.md              # /review - code review (read-only)
```

## Setup

### 1. Copy Files to Your Project

```bash
# From wherever you downloaded these files:
cp CLAUDE.md /path/to/your-project/
cp -r .claude /path/to/your-project/
```

### 2. Verify Structure

```bash
cd /path/to/your-project
ls -la CLAUDE.md .claude/commands/
```

You should see:
- `CLAUDE.md` in project root
- 5 command files in `.claude/commands/`

### 3. Start Using

Open your project in VS Code with Claude Code extension.

## Commands

### /feature [description]
Full SDLC for new features:
1. Clarify requirements + security classification
2. Design approach
3. Implement code
4. Verify (security scan + tests)
5. Deliver summary

**Example:**
```
/feature Add user authentication with email/password
```

### /bugfix [description]
Lighter process for bug fixes:
1. Clarify the bug
2. Implement fix
3. Verify + add regression test
4. Deliver summary

**Example:**
```
/bugfix Users can access other users' profiles by changing URL ID
```

### /security [optional: path]
Security review without modification:
- Runs available security tools
- Checks for hardcoded secrets
- Manual review checklist
- Reports all findings

**Example:**
```
/security
/security src/auth/
```

### /qa [optional: path]
Quality assurance:
- Runs test suite
- Checks coverage
- Runs linters
- Identifies test gaps

**Example:**
```
/qa
/qa src/services/
```

### /review [file or path]
Code review (read-only):
- Analyzes code quality
- Identifies issues
- Suggests improvements
- Does NOT modify code

**Example:**
```
/review src/api/users.py
/review src/
```

## How It Works

### CLAUDE.md
Claude Code automatically reads `CLAUDE.md` at the start of every conversation. This file defines:
- Mandatory SDLC phases
- Security classification rules
- Quality standards
- Auto-fix behavior

### Commands
The `.claude/commands/` folder contains templates that activate when you type the command. Each command adds specific instructions on top of `CLAUDE.md`.

### Security Classification

Every task gets classified:
- 🔴 **CRITICAL**: auth, payments, crypto, PII, secrets
- 🟠 **HIGH**: user data, sessions, uploads, external APIs
- 🟡 **STANDARD**: business logic, internal APIs
- 🟢 **LOW**: docs, styling, config

CRITICAL/HIGH tasks get more scrutiny (design required, questions asked).

## Customization

### Add Project-Specific Rules

Edit `CLAUDE.md` to add your project's conventions:

```markdown
## Project-Specific Rules

### Tech Stack
- Backend: Python/FastAPI
- Database: PostgreSQL
- Auth: JWT tokens

### Conventions
- Use snake_case for Python
- All API endpoints under /api/v1/
- Environment variables in .env (never committed)
```

### Add Custom Commands

Create new files in `.claude/commands/`:

```markdown
# /deploy Command

Trigger: User types `/deploy [environment]`

## Your Task
[Define what this command does]
```

## Limitations (Why Option B Exists)

This is "soft enforcement" — Claude Code follows instructions but:
- May drift on very long conversations
- No external audit trail
- No hard gates (can't truly block progress)
- All in one context window

If you need harder enforcement, deterministic flow, or audit logging, see Option B (orchestrator).

## Troubleshooting

### Commands Not Working
- Ensure `.claude/commands/` folder exists
- Check file names match command names
- Restart Claude Code

### CLAUDE.md Not Loading
- Must be in project root
- Must be named exactly `CLAUDE.md`
- Check for syntax errors

### Security Tools Not Found
The security checks will skip unavailable tools. Install what you need:

**Python:**
```bash
pip install bandit pip-audit safety ruff mypy pytest pytest-cov
```

**JavaScript:**
```bash
npm install -D eslint jest
```
