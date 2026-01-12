#!/usr/bin/env python3
"""
SDLC Orchestrator for Claude Code

Runs a structured software development lifecycle using Claude Code CLI.
Each phase is a fresh Claude instance with focused context.
Human approval required between phases.

Usage:
    python orchestrator.py "Add user authentication"
    python orchestrator.py --resume task-abc123
"""

import subprocess
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional
from enum import Enum


# =============================================================================
# Configuration
# =============================================================================

class Config:
    # Where to store pipeline artifacts
    PIPELINE_DIR = ".claude/pipeline"
    
    # Where prompts live
    PROMPTS_DIR = ".claude/prompts"
    
    # Max attempts to fix issues before failing
    MAX_FIX_ATTEMPTS = 3
    
    # Security classification that requires extra scrutiny
    CRITICAL_CLASSIFICATIONS = ["CRITICAL", "HIGH"]


# =============================================================================
# Data Models
# =============================================================================

class Phase(Enum):
    REQUIREMENTS = "requirements"
    ARCHITECTURE = "architecture"
    IMPLEMENTATION = "implementation"
    SECURITY = "security"
    QA = "qa"
    COMPLETE = "complete"


class SecurityLevel(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    STANDARD = "STANDARD"
    LOW = "LOW"


@dataclass
class TaskState:
    task_id: str
    description: str
    created_at: str
    current_phase: str
    security_level: Optional[str] = None
    phases_completed: list = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.phases_completed is None:
            self.phases_completed = []


# =============================================================================
# File Helpers
# =============================================================================

def get_task_dir(task_id: str) -> Path:
    """Get the directory for a specific task."""
    return Path(Config.PIPELINE_DIR) / "tasks" / task_id


def save_state(state: TaskState):
    """Save task state to disk."""
    task_dir = get_task_dir(state.task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    
    state_file = task_dir / "state.json"
    with open(state_file, "w") as f:
        json.dump(asdict(state), f, indent=2)


def load_state(task_id: str) -> Optional[TaskState]:
    """Load task state from disk."""
    state_file = get_task_dir(task_id) / "state.json"
    
    if not state_file.exists():
        return None
    
    with open(state_file) as f:
        data = json.load(f)
    
    return TaskState(**data)


def save_artifact(task_id: str, name: str, content: str):
    """Save a phase artifact."""
    task_dir = get_task_dir(task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    
    artifact_file = task_dir / name
    with open(artifact_file, "w") as f:
        f.write(content)
    
    print(f"  💾 Saved: {artifact_file}")


def load_artifact(task_id: str, name: str) -> Optional[str]:
    """Load a phase artifact."""
    artifact_file = get_task_dir(task_id) / name
    
    if not artifact_file.exists():
        return None
    
    with open(artifact_file) as f:
        return f.read()


def get_prompt(phase: str) -> str:
    """Load prompt for a phase."""
    prompt_file = Path(Config.PROMPTS_DIR) / f"{phase}.md"
    
    if not prompt_file.exists():
        raise FileNotFoundError(f"Prompt not found: {prompt_file}")
    
    with open(prompt_file) as f:
        return f.read()


# =============================================================================
# Claude Code Integration
# =============================================================================

def call_claude(prompt: str, context: str = "", working_dir: str = ".") -> tuple[bool, str]:
    """
    Call Claude Code CLI with a prompt.
    
    Returns (success, output)
    """
    full_prompt = prompt
    if context:
        full_prompt = f"{context}\n\n---\n\n{prompt}"
    
    try:
        # Claude Code CLI command
        # Adjust based on your actual CLI syntax
        result = subprocess.run(
            ["claude", "-p", full_prompt, "--no-input"],
            capture_output=True,
            text=True,
            cwd=working_dir,
            timeout=300  # 5 minute timeout per phase
        )
        
        output = result.stdout
        if result.stderr:
            output += f"\n\nSTDERR:\n{result.stderr}"
        
        return result.returncode == 0, output
        
    except subprocess.TimeoutExpired:
        return False, "ERROR: Claude Code timed out after 5 minutes"
    except FileNotFoundError:
        return False, "ERROR: Claude Code CLI not found. Is 'claude' in your PATH?"
    except Exception as e:
        return False, f"ERROR: {str(e)}"


def call_claude_interactive(prompt: str, context: str = "", working_dir: str = ".") -> tuple[bool, str]:
    """
    Call Claude Code in interactive mode (for implementation phase).
    This lets Claude make file changes.
    """
    full_prompt = prompt
    if context:
        full_prompt = f"{context}\n\n---\n\n{prompt}"
    
    try:
        # For implementation, we want Claude to actually modify files
        # This might need adjustment based on your Claude Code setup
        result = subprocess.run(
            ["claude", full_prompt],
            capture_output=True,
            text=True,
            cwd=working_dir,
            timeout=600  # 10 minutes for implementation
        )
        
        output = result.stdout
        if result.stderr:
            output += f"\n\nSTDERR:\n{result.stderr}"
        
        return result.returncode == 0, output
        
    except subprocess.TimeoutExpired:
        return False, "ERROR: Claude Code timed out"
    except Exception as e:
        return False, f"ERROR: {str(e)}"


# =============================================================================
# Security Tools (NOT Claude - actual tools)
# =============================================================================

def run_security_scan(working_dir: str = ".") -> dict:
    """
    Run actual security tools (not Claude reviewing).
    Returns dict with results.
    """
    results = {
        "tools_run": [],
        "findings": [],
        "passed": True
    }
    
    # Detect project type
    has_python = Path(working_dir, "requirements.txt").exists() or \
                 Path(working_dir, "pyproject.toml").exists() or \
                 list(Path(working_dir).glob("**/*.py"))
    
    has_node = Path(working_dir, "package.json").exists()
    
    # Run bandit (Python)
    if has_python:
        try:
            result = subprocess.run(
                ["bandit", "-r", ".", "-f", "json", "-q"],
                capture_output=True,
                text=True,
                cwd=working_dir,
                timeout=120
            )
            results["tools_run"].append("bandit")
            
            if result.stdout:
                bandit_results = json.loads(result.stdout)
                for issue in bandit_results.get("results", []):
                    results["findings"].append({
                        "tool": "bandit",
                        "severity": issue.get("issue_severity", "MEDIUM"),
                        "file": issue.get("filename", "unknown"),
                        "line": issue.get("line_number", 0),
                        "description": issue.get("issue_text", "")
                    })
                    if issue.get("issue_severity") in ["HIGH", "CRITICAL"]:
                        results["passed"] = False
                        
        except FileNotFoundError:
            results["tools_run"].append("bandit (not installed)")
        except Exception as e:
            results["tools_run"].append(f"bandit (error: {e})")
    
    # Run pip-audit (Python)
    if has_python:
        try:
            result = subprocess.run(
                ["pip-audit", "--format", "json"],
                capture_output=True,
                text=True,
                cwd=working_dir,
                timeout=120
            )
            results["tools_run"].append("pip-audit")
            
            if result.stdout:
                audit_results = json.loads(result.stdout)
                for vuln in audit_results:
                    results["findings"].append({
                        "tool": "pip-audit",
                        "severity": "HIGH",
                        "file": "requirements",
                        "line": 0,
                        "description": f"{vuln.get('name')}: {vuln.get('vulns', [])}"
                    })
                    results["passed"] = False
                    
        except FileNotFoundError:
            results["tools_run"].append("pip-audit (not installed)")
        except Exception as e:
            results["tools_run"].append(f"pip-audit (error: {e})")
    
    # Run npm audit (Node)
    if has_node:
        try:
            result = subprocess.run(
                ["npm", "audit", "--json"],
                capture_output=True,
                text=True,
                cwd=working_dir,
                timeout=120
            )
            results["tools_run"].append("npm-audit")
            
            if result.stdout:
                npm_results = json.loads(result.stdout)
                for vuln_id, vuln in npm_results.get("vulnerabilities", {}).items():
                    results["findings"].append({
                        "tool": "npm-audit",
                        "severity": vuln.get("severity", "medium").upper(),
                        "file": "package.json",
                        "line": 0,
                        "description": f"{vuln_id}: {vuln.get('title', '')}"
                    })
                    if vuln.get("severity") in ["high", "critical"]:
                        results["passed"] = False
                        
        except FileNotFoundError:
            results["tools_run"].append("npm-audit (not installed)")
        except Exception as e:
            results["tools_run"].append(f"npm-audit (error: {e})")
    
    # Always run: grep for secrets
    secret_patterns = [
        (r"password\s*=\s*['\"][^'\"]{8,}['\"]", "Hardcoded password"),
        (r"api_key\s*=\s*['\"][^'\"]{16,}['\"]", "Hardcoded API key"),
        (r"secret\s*=\s*['\"][^'\"]{8,}['\"]", "Hardcoded secret"),
        (r"AKIA[A-Z0-9]{16}", "AWS Access Key"),
    ]
    
    results["tools_run"].append("secret-scan")
    
    for pattern, description in secret_patterns:
        try:
            result = subprocess.run(
                ["grep", "-rn", "-E", pattern, 
                 "--include=*.py", "--include=*.js", "--include=*.ts",
                 "--include=*.go", "--include=*.java", "--include=*.rb",
                 "."],
                capture_output=True,
                text=True,
                cwd=working_dir
            )
            
            if result.stdout:
                for line in result.stdout.strip().split("\n"):
                    if line and "node_modules" not in line and ".git" not in line:
                        parts = line.split(":", 2)
                        results["findings"].append({
                            "tool": "secret-scan",
                            "severity": "CRITICAL",
                            "file": parts[0] if len(parts) > 0 else "unknown",
                            "line": int(parts[1]) if len(parts) > 1 else 0,
                            "description": description
                        })
                        results["passed"] = False
                        
        except Exception:
            pass
    
    return results


# =============================================================================
# Test Runner (NOT Claude - actual tests)
# =============================================================================

def run_tests(working_dir: str = ".") -> dict:
    """
    Run actual test framework.
    Returns dict with results.
    """
    results = {
        "framework": None,
        "passed": False,
        "total": 0,
        "failures": 0,
        "output": ""
    }
    
    # Try pytest
    try:
        result = subprocess.run(
            ["pytest", "-v", "--tb=short"],
            capture_output=True,
            text=True,
            cwd=working_dir,
            timeout=300
        )
        
        results["framework"] = "pytest"
        results["output"] = result.stdout + result.stderr
        results["passed"] = result.returncode == 0
        
        # Parse pytest output for counts
        for line in result.stdout.split("\n"):
            if "passed" in line or "failed" in line:
                # Rough parsing - could be improved
                import re
                match = re.search(r"(\d+) passed", line)
                if match:
                    results["total"] += int(match.group(1))
                match = re.search(r"(\d+) failed", line)
                if match:
                    results["failures"] = int(match.group(1))
                    results["total"] += int(match.group(1))
        
        return results
        
    except FileNotFoundError:
        pass
    
    # Try npm test
    if Path(working_dir, "package.json").exists():
        try:
            result = subprocess.run(
                ["npm", "test"],
                capture_output=True,
                text=True,
                cwd=working_dir,
                timeout=300
            )
            
            results["framework"] = "npm"
            results["output"] = result.stdout + result.stderr
            results["passed"] = result.returncode == 0
            
            return results
            
        except Exception:
            pass
    
    results["framework"] = "none"
    results["output"] = "No test framework detected"
    results["passed"] = True  # No tests = pass (for now)
    
    return results


# =============================================================================
# Phase Executors
# =============================================================================

def execute_requirements(state: TaskState) -> bool:
    """Phase 1: Clarify requirements."""
    print("\n" + "="*60)
    print("📋 PHASE 1: REQUIREMENTS")
    print("="*60)
    
    prompt = get_prompt("requirements")
    prompt = prompt.replace("{{TASK}}", state.description)
    
    success, output = call_claude(prompt)
    
    if not success:
        print(f"❌ Failed: {output}")
        state.error = output
        return False
    
    save_artifact(state.task_id, "requirements.md", output)
    
    # Try to extract security level from output
    for level in SecurityLevel:
        if level.value in output.upper():
            state.security_level = level.value
            break
    
    if not state.security_level:
        state.security_level = SecurityLevel.STANDARD.value
    
    print(f"\n🔒 Security Classification: {state.security_level}")
    print(f"\n📄 Requirements captured. Review: {get_task_dir(state.task_id)}/requirements.md")
    
    return True


def execute_architecture(state: TaskState) -> bool:
    """Phase 2: Design architecture."""
    print("\n" + "="*60)
    print("🏗️  PHASE 2: ARCHITECTURE")
    print("="*60)
    
    # Load requirements as context
    requirements = load_artifact(state.task_id, "requirements.md")
    
    prompt = get_prompt("architecture")
    
    success, output = call_claude(prompt, context=f"# Requirements\n\n{requirements}")
    
    if not success:
        print(f"❌ Failed: {output}")
        state.error = output
        return False
    
    save_artifact(state.task_id, "architecture.md", output)
    
    print(f"\n📄 Architecture documented. Review: {get_task_dir(state.task_id)}/architecture.md")
    
    return True


def execute_implementation(state: TaskState) -> bool:
    """Phase 3: Implement the solution."""
    print("\n" + "="*60)
    print("💻 PHASE 3: IMPLEMENTATION")
    print("="*60)
    
    # Load architecture as context
    architecture = load_artifact(state.task_id, "architecture.md")
    
    prompt = get_prompt("implementation")
    
    print("  Claude Code is implementing... (this may take a few minutes)")
    
    success, output = call_claude_interactive(prompt, context=f"# Architecture\n\n{architecture}")
    
    if not success:
        print(f"❌ Failed: {output}")
        state.error = output
        return False
    
    save_artifact(state.task_id, "implementation.log", output)
    
    print(f"\n📄 Implementation complete. Log: {get_task_dir(state.task_id)}/implementation.log")
    
    return True


def execute_security(state: TaskState) -> bool:
    """Phase 4: Security verification (actual tools, not Claude)."""
    print("\n" + "="*60)
    print("🔒 PHASE 4: SECURITY VERIFICATION")
    print("="*60)
    
    for attempt in range(Config.MAX_FIX_ATTEMPTS):
        print(f"\n  Scan attempt {attempt + 1}/{Config.MAX_FIX_ATTEMPTS}")
        
        results = run_security_scan()
        
        print(f"  Tools run: {', '.join(results['tools_run'])}")
        print(f"  Findings: {len(results['findings'])}")
        
        if results["passed"]:
            print("  ✅ Security scan PASSED")
            save_artifact(state.task_id, "security.json", json.dumps(results, indent=2))
            return True
        
        # Show findings
        print("\n  ❌ Security issues found:")
        for finding in results["findings"]:
            print(f"    [{finding['severity']}] {finding['file']}:{finding['line']} - {finding['description']}")
        
        if attempt < Config.MAX_FIX_ATTEMPTS - 1:
            print("\n  Asking Claude to fix...")
            
            fix_prompt = f"""Security scan found these issues. Fix them:

{json.dumps(results['findings'], indent=2)}

Fix each issue and respond with what you changed."""
            
            success, output = call_claude_interactive(fix_prompt)
            
            if not success:
                print(f"  Fix attempt failed: {output}")
    
    print(f"\n❌ Security issues remain after {Config.MAX_FIX_ATTEMPTS} attempts")
    save_artifact(state.task_id, "security.json", json.dumps(results, indent=2))
    state.error = f"Security scan failed with {len(results['findings'])} issues"
    
    return False


def execute_qa(state: TaskState) -> bool:
    """Phase 5: Quality assurance (actual tests, not Claude)."""
    print("\n" + "="*60)
    print("🧪 PHASE 5: QA VERIFICATION")
    print("="*60)
    
    for attempt in range(Config.MAX_FIX_ATTEMPTS):
        print(f"\n  Test attempt {attempt + 1}/{Config.MAX_FIX_ATTEMPTS}")
        
        results = run_tests()
        
        print(f"  Framework: {results['framework']}")
        print(f"  Total tests: {results['total']}")
        print(f"  Failures: {results['failures']}")
        
        if results["passed"]:
            print("  ✅ Tests PASSED")
            save_artifact(state.task_id, "qa.json", json.dumps(results, indent=2))
            return True
        
        if results["framework"] == "none":
            print("  ⚠️  No tests found - consider adding tests")
            save_artifact(state.task_id, "qa.json", json.dumps(results, indent=2))
            return True  # Pass for now, but note the warning
        
        print(f"\n  ❌ Tests failed")
        
        if attempt < Config.MAX_FIX_ATTEMPTS - 1:
            print("\n  Asking Claude to fix...")
            
            fix_prompt = f"""Tests are failing. Here's the output:

{results['output'][:2000]}

Fix the failing tests or the code causing them to fail."""
            
            success, output = call_claude_interactive(fix_prompt)
            
            if not success:
                print(f"  Fix attempt failed: {output}")
    
    print(f"\n❌ Tests still failing after {Config.MAX_FIX_ATTEMPTS} attempts")
    save_artifact(state.task_id, "qa.json", json.dumps(results, indent=2))
    state.error = f"Tests failed: {results['failures']} failures"
    
    return False


# =============================================================================
# Human Gates
# =============================================================================

def ask_continue(phase_name: str, next_phase: str) -> bool:
    """Ask human whether to continue to next phase."""
    print("\n" + "-"*60)
    print(f"✅ {phase_name} complete")
    print(f"➡️  Next phase: {next_phase}")
    print("-"*60)
    
    while True:
        response = input("\nContinue to next phase? [y/n/review]: ").strip().lower()
        
        if response in ["y", "yes"]:
            return True
        elif response in ["n", "no"]:
            print("Pipeline paused. Run with --resume to continue later.")
            return False
        elif response == "review":
            print("Review the artifacts in .claude/pipeline/tasks/<task-id>/")
            print("Then type 'y' to continue or 'n' to pause.")
        else:
            print("Please enter 'y', 'n', or 'review'")


# =============================================================================
# Main Pipeline
# =============================================================================

def run_pipeline(description: str, resume_task_id: Optional[str] = None):
    """Run the full SDLC pipeline."""
    
    # Initialize or resume state
    if resume_task_id:
        state = load_state(resume_task_id)
        if not state:
            print(f"❌ Task not found: {resume_task_id}")
            return False
        print(f"📂 Resuming task: {state.task_id}")
    else:
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        state = TaskState(
            task_id=task_id,
            description=description,
            created_at=datetime.now().isoformat(),
            current_phase=Phase.REQUIREMENTS.value
        )
        print(f"📂 New task: {state.task_id}")
    
    save_state(state)
    
    print(f"\n📝 Task: {state.description}")
    print(f"📁 Artifacts: {get_task_dir(state.task_id)}")
    
    # Phase execution order
    phases = [
        (Phase.REQUIREMENTS, execute_requirements, "REQUIREMENTS", "ARCHITECTURE"),
        (Phase.ARCHITECTURE, execute_architecture, "ARCHITECTURE", "IMPLEMENTATION"),
        (Phase.IMPLEMENTATION, execute_implementation, "IMPLEMENTATION", "SECURITY"),
        (Phase.SECURITY, execute_security, "SECURITY", "QA"),
        (Phase.QA, execute_qa, "QA", "COMPLETE"),
    ]
    
    # Find where to resume
    start_index = 0
    for i, (phase, _, _, _) in enumerate(phases):
        if phase.value == state.current_phase:
            start_index = i
            break
    
    # Execute phases
    for i in range(start_index, len(phases)):
        phase, executor, name, next_name = phases[i]
        
        state.current_phase = phase.value
        save_state(state)
        
        # Execute the phase
        success = executor(state)
        
        if not success:
            print(f"\n❌ Pipeline failed at {name}")
            save_state(state)
            return False
        
        # Mark phase complete
        if phase.value not in state.phases_completed:
            state.phases_completed.append(phase.value)
        save_state(state)
        
        # Human gate (except after last phase)
        if i < len(phases) - 1:
            if not ask_continue(name, next_name):
                return False
    
    # Complete!
    state.current_phase = Phase.COMPLETE.value
    save_state(state)
    
    print("\n" + "="*60)
    print("🎉 PIPELINE COMPLETE")
    print("="*60)
    print(f"\nTask: {state.description}")
    print(f"Security Level: {state.security_level}")
    print(f"Phases Completed: {', '.join(state.phases_completed)}")
    print(f"\nArtifacts: {get_task_dir(state.task_id)}")
    
    return True


# =============================================================================
# CLI
# =============================================================================

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python orchestrator.py \"Task description\"")
        print("  python orchestrator.py --resume task-abc123")
        print("  python orchestrator.py --list")
        sys.exit(1)
    
    if sys.argv[1] == "--list":
        # List all tasks
        tasks_dir = Path(Config.PIPELINE_DIR) / "tasks"
        if tasks_dir.exists():
            for task_dir in tasks_dir.iterdir():
                state = load_state(task_dir.name)
                if state:
                    print(f"{state.task_id}: {state.current_phase} - {state.description[:50]}")
        else:
            print("No tasks found")
        return
    
    if sys.argv[1] == "--resume":
        if len(sys.argv) < 3:
            print("Usage: python orchestrator.py --resume task-abc123")
            sys.exit(1)
        run_pipeline("", resume_task_id=sys.argv[2])
    else:
        description = " ".join(sys.argv[1:])
        run_pipeline(description)


if __name__ == "__main__":
    main()
