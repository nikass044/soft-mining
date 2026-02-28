from __future__ import annotations


class GitHubApiError(Exception):
    def __init__(self, status_code: int, message: str, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class TransientError(GitHubApiError):
    pass


class RateLimitError(TransientError):
    def __init__(self, status_code: int, message: str, retry_after: float | None = None, body: str = ""):
        super().__init__(status_code, message, body)
        self.retry_after = retry_after


class PermanentError(GitHubApiError):
    pass
