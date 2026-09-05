"""Child process fixture that deliberately leaves its payload pipe unread."""

from __future__ import annotations

import time


def main() -> None:
    """Stay alive briefly so launcher timeout and cancellation cleanup are tested."""

    time.sleep(3)


if __name__ == "__main__":
    main()
