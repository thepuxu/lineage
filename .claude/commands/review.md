# /review Command

Trigger: User types `/review [file, directory, or description]`

## Your Task

Code review mode — analyze and report, but **do NOT modify** anything.

## Process

### 1. Understand Scope

If given:
- **File**: Review that specific file
- **Directory**: Review all code files in that directory
- **Description**: Find relevant files and review them
- **Nothing**: Review recent changes or ask what to review

### 2. Review Dimensions

Analyze code across these dimensions:

**Correctness**
- Does the logic do what it's supposed to?
- Are there edge cases not handled?
- Are there potential runtime errors?

**Security**
- Any vulnerabilities? (injection, auth issues, data exposure)
- Proper input validation?
- Secure handling of sensitive data?

**Maintainability**
- Clear naming and structure?
- Appropriate abstractions?
- Easy to understand and modify?

**Performance**
- Any obvious inefficiencies?
- N+1 queries?
- Unnecessary computations?

**Testing**
- Is this code testable?
- What tests would you recommend?

### 3. Report Format

```markdown
## 📝 Code Review: [file/area]

### Summary
[2-3 sentence overall assessment]

### 🔴 Critical Issues
[Must fix - bugs, security vulnerabilities]

1. **[Location]**: [Issue]
   - Problem: [explanation]
   - Suggestion: [how to fix]

### 🟠 Important Suggestions
[Should fix - significant improvements]

1. **[Location]**: [Issue]
   - Current: [what it does now]
   - Better: [what it should do]

### 🟡 Minor Suggestions
[Nice to have - style, minor improvements]

1. **[Location]**: [suggestion]

### ✅ What's Good
[Acknowledge good patterns/decisions]

- [Good thing 1]
- [Good thing 2]

### Test Recommendations
If I were to add tests, I'd focus on:
1. [Test case 1]
2. [Test case 2]

### Questions for Author
- [Clarifying question about intent]
```

### 4. Don't Modify

Remember: This is READ-ONLY analysis.

Say at the end:
"This is a review only — I haven't changed any code. 
Would you like me to implement any of these suggestions?"

## Start

Identify what to review and begin analysis.
