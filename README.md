# pr-digger

Mines GitHub PR metadata, changed files, and reviews into a local SQLite database.

## Setup

```bash
pip3 install -e ".[dev]"
```

Add your GitHub token to `.env`:

```
GITHUB_TOKEN=ghp_...
```

Configure repos and earliest date in `pr_digger/config.py`.

## Run

```bash
python3 -m pr_digger.app --all        # all tasks (file + review mining run in parallel)
python3 -m pr_digger.app --prs        # mine PR metadata and users
python3 -m pr_digger.app --files      # mine PR files (GraphQL)
python3 -m pr_digger.app --reviews    # mine PR reviews
```

## Inspect the database

```bash
sqlite3 data/pr_digger.db

-- PR counts per repo
SELECT r.full_name, COUNT(*) FROM pull_requests pr JOIN repositories r ON r.id = pr.repo_id GROUP BY r.id;

-- Files most frequently changed
SELECT f.path, COUNT(*) c FROM pull_request_files pf JOIN files f ON f.id = pf.file_id GROUP BY f.id ORDER BY c DESC LIMIT 20;

-- Reviews per PR
SELECT pr.number, COUNT(*) FROM pull_request_reviews rev JOIN pull_requests pr ON pr.id = rev.pull_request_id GROUP BY pr.id ORDER BY COUNT(*) DESC LIMIT 10;
```

## Tests

```bash
python3 -m pytest tests/ -v
```
