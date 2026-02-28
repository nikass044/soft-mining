from pr_digger.parser import PayloadParser


class TestParsePRList:
    def test_extracts_users_and_prs(self):
        payload = [
            {
                "number": 1,
                "state": "closed",
                "created_at": "2024-01-01T00:00:00Z",
                "merged_at": "2024-01-02T00:00:00Z",
                "closed_at": "2024-01-02T00:00:00Z",
                "user": {"id": 42, "login": "author1"},
            },
            {
                "number": 2,
                "state": "open",
                "created_at": "2024-01-03T00:00:00Z",
                "merged_at": None,
                "closed_at": None,
                "user": {"id": 99, "login": "author2"},
            },
        ]
        parser = PayloadParser()
        batch = parser.parse_pr_list(payload, repo_id=1)

        assert len(batch.users) == 2
        assert batch.users[0].github_user_id == 42
        assert len(batch.pull_requests) == 2
        assert batch.pull_requests[0].number == 1
        assert batch.pull_requests[1].state == "open"


class TestParsePRFiles:
    def test_extracts_file_paths(self):
        payload = {
            "data": {
                "repository": {
                    "pullRequest": {
                        "number": 1,
                        "files": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [
                                {"path": "src/index.js"},
                                {"path": "README.md"},
                            ],
                        },
                    }
                }
            }
        }
        parser = PayloadParser()
        batch = parser.parse_pr_files(payload, repo_id=1, pull_request_id=10)
        assert batch.file_paths == ["src/index.js", "README.md"]

    def test_page_info_extraction(self):
        payload = {
            "data": {
                "repository": {
                    "pullRequest": {
                        "files": {
                            "pageInfo": {"hasNextPage": True, "endCursor": "abc123"},
                            "nodes": [],
                        }
                    }
                }
            }
        }
        parser = PayloadParser()
        has_next, cursor = parser.parse_pr_files_page_info(payload)
        assert has_next is True
        assert cursor == "abc123"


class TestParsePRReviews:
    def test_extracts_reviews_and_users(self):
        payload = [
            {
                "id": 501,
                "state": "APPROVED",
                "submitted_at": "2024-01-05T00:00:00Z",
                "user": {"id": 77, "login": "reviewer1"},
            },
            {
                "id": 502,
                "state": "CHANGES_REQUESTED",
                "submitted_at": "2024-01-06T00:00:00Z",
                "user": {"id": 88, "login": "reviewer2"},
            },
        ]
        parser = PayloadParser()
        batch = parser.parse_pr_reviews(payload, pull_request_id=10)

        assert len(batch.users) == 2
        assert batch.users[0].login == "reviewer1"
        assert len(batch.reviews) == 2
        assert batch.reviews[0].github_review_id == 501
        assert batch.reviews[1].state == "CHANGES_REQUESTED"
