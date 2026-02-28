# pr-digger

Mines GitHub PR metadata, changed files, and reviews into a local SQLite database.

## Setup

```bash
pip3 install -e ".[dev]"
```

Set your GitHub token and repos in `pr_digger/config.py`.

## Run

```bash
python3 -m pr_digger.app --all              # all phases
python3 -m pr_digger.app --phase1           # PR metadata only
python3 -m pr_digger.app --phase2           # PR files (GraphQL)
python3 -m pr_digger.app --phase3           # PR reviews
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
