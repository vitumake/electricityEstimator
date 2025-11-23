from pathlib import Path
import os


def load_dotenv(dotenv_path: str | Path = ".env") -> None:
    """Minimal .env loader: KEY=VALUE lines, ignores comments/blank lines.

    Does not override environment variables that are already set.
    """

    path = Path(dotenv_path)
    if not path.exists():
        return

    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        # Do not override already-set env vars
        os.environ.setdefault(key, value)
