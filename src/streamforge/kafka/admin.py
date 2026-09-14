"""CLI entrypoint alias for Kafka cluster administration."""

import sys

from streamforge.shared.kafka.admin import (
    build_admin_client,
    create_topics,
    describe_cluster,
    describe_topics,
    get_default_topics,
    main,
)

__all__ = [
    "build_admin_client",
    "create_topics",
    "describe_cluster",
    "describe_topics",
    "get_default_topics",
    "main",
]

if __name__ == "__main__":
    sys.exit(main())
