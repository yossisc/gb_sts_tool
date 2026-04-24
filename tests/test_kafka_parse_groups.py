"""Unit tests for consumer-group topic parsing and du aggregation helpers."""

from __future__ import annotations

import unittest

from backend import kafka_parse as kp


class TestKafkaParseConsumerGroups(unittest.TestCase):
    def test_parse_consumer_group_assigned_topics_basic(self) -> None:
        desc = """
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
elastic_offline_group elastic_offline 0 1 2 1 consumer-1 host-1 client-1
elastic_offline_group elastic_offline 1 1 2 1 consumer-1 host-1 client-1
other_group t_other 0 0 0 0 consumer-2 host-2 client-2
""".strip()
        got = kp.parse_consumer_group_assigned_topics(desc)
        self.assertEqual(got["elastic_offline_group"], {"elastic_offline"})
        self.assertEqual(got["other_group"], {"t_other"})

    def test_topic_total_kb_from_du_stdout(self) -> None:
        du = "1024000\t/bitnami/kafka/data/elastic_offline-0\n512000\t/bitnami/kafka/data/elastic_offline-1\n"
        got = kp.topic_total_kb_from_du_stdout(du)
        self.assertEqual(got.get("elastic_offline"), 1024000 + 512000)


if __name__ == "__main__":
    unittest.main()
