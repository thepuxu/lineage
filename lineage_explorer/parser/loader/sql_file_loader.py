"""
Load SQL files for lineage parsing.
"""

from pathlib import Path
from typing import List, Tuple, Optional
import re


class SQLFileLoader:
    """Load and preprocess SQL files."""

    def __init__(self):
        self.files: List[Tuple[str, str, Path]] = []  # [(object_name, sql_content, path), ...]

    def load_file(self, path: Path) -> Tuple[str, str]:
        """
        Load a single SQL file.

        Args:
            path: Path to SQL file

        Returns:
            Tuple of (object_name, sql_content)
        """
        path = Path(path)

        # Object name is filename without extension, uppercased
        object_name = path.stem.upper()

        # Read file content
        with open(path, encoding="utf-8", errors="replace") as f:
            sql_content = f.read()

        self.files.append((object_name, sql_content, path))

        return object_name, sql_content

    def load_directory(self, dir_path: Path, pattern: str = "*.sql") -> List[Tuple[str, str]]:
        """
        Load all SQL files from a directory.

        Args:
            dir_path: Directory path
            pattern: Glob pattern for files (default: *.sql)

        Returns:
            List of (object_name, sql_content) tuples
        """
        dir_path = Path(dir_path)

        if not dir_path.is_dir():
            raise ValueError(f"Not a directory: {dir_path}")

        results = []

        # Support multiple patterns
        patterns = [pattern]
        if pattern == "*.sql":
            patterns.extend(["*.txt", "*.SQL", "*.TXT"])

        for pat in patterns:
            for file_path in dir_path.glob(pat):
                if file_path.is_file():
                    obj_name, sql = self.load_file(file_path)
                    results.append((obj_name, sql))

        print(f"Loaded {len(results)} SQL files from {dir_path}")

        return results

    def get_files(self) -> List[Tuple[str, str, Path]]:
        """Get all loaded files."""
        return self.files

    @staticmethod
    def extract_select(sql: str) -> Optional[str]:
        """
        Extract the main SELECT statement from SQL.

        The SQL files contain only SELECT portions (not full INSERT/MERGE).
        This normalizes the SQL for parsing.
        """
        # First strip comments to avoid matching SELECT in comments
        sql_clean = SQLFileLoader.normalize_sql(sql)

        # If it starts with SELECT, return cleaned version
        if sql_clean.upper().startswith("SELECT"):
            return sql_clean

        # If it starts with WITH (CTE), return cleaned version
        if sql_clean.upper().startswith("WITH"):
            return sql_clean

        # Try to find SELECT in the cleaned content
        match = re.search(r'\b(SELECT\b.*)', sql_clean, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)

        # Try to find WITH ... SELECT
        match = re.search(r'\b(WITH\b.*)', sql_clean, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)

        return sql_clean

    @staticmethod
    def normalize_sql(sql: str) -> str:
        """
        Normalize SQL for parsing:
        - Strip comments
        - Normalize whitespace
        - Preserve string literals
        """
        result = []
        i = 0
        in_string = False
        in_line_comment = False
        in_block_comment = False

        while i < len(sql):
            char = sql[i]

            # Handle string literals
            if char == "'" and not in_line_comment and not in_block_comment:
                if in_string:
                    # Check for escaped quote ''
                    if i + 1 < len(sql) and sql[i + 1] == "'":
                        result.append("''")
                        i += 2
                        continue
                    else:
                        in_string = False
                else:
                    in_string = True
                result.append(char)
                i += 1
                continue

            # Inside string - preserve as-is
            if in_string:
                result.append(char)
                i += 1
                continue

            # Handle line comments (-- ...)
            if char == "-" and i + 1 < len(sql) and sql[i + 1] == "-":
                if not in_block_comment:
                    in_line_comment = True
                    i += 2
                    continue

            if in_line_comment:
                if char == "\n":
                    in_line_comment = False
                    result.append(" ")
                i += 1
                continue

            # Handle block comments (/* ... */)
            if char == "/" and i + 1 < len(sql) and sql[i + 1] == "*":
                in_block_comment = True
                i += 2
                continue

            if in_block_comment:
                if char == "*" and i + 1 < len(sql) and sql[i + 1] == "/":
                    in_block_comment = False
                    result.append(" ")
                    i += 2
                    continue
                i += 1
                continue

            # Normalize whitespace
            if char in " \t\n\r":
                if result and result[-1] != " ":
                    result.append(" ")
            else:
                result.append(char)

            i += 1

        return "".join(result).strip()
