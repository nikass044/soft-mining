# pr-digger

Mines GitHub PR metadata, changed files, and reviews into a local SQLite database.

## Setup

```bash
pip3 install -e ".[dev]"
```

Create a `.env` file in the root of the project and add your GitHub token to id:

```
GITHUB_TOKEN=ghp_...
```

Uncomment your chunk in the REPOS list `pr_digger/config.py`.

## Run

```bash
python3 -m pr_digger.app --all        # all tasks (file + review mining run in parallel)
```
