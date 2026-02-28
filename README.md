# pr-digger

Mines GitHub PR metadata, changed files, and reviews into a local SQLite database.

## Setup

```bash
pip3 install -e ".[dev]"
```

Create a `.env` file in the project root:

```
GITHUB_TOKEN=ghp_yourTokenHere
```

## Run

```bash
python3 -m pr_digger.app
```

Run specific phases only:

```bash
PR_DIGGER_PHASES=1 python3 -m pr_digger.app        # PR metadata only
PR_DIGGER_PHASES=1,2 python3 -m pr_digger.app      # metadata + files
```

Override the target repo:

```bash
PR_DIGGER_REPOS=facebook/react,vercel/next.js python3 -m pr_digger.app
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
