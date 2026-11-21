import json

from producer import DeliveryStats, build_key, build_value

EVENT = {
    "wiki": "trwiktionary",
    "type": "categorize",
    "title": "Kategori:Portekizce çekimli eylemler",
    "user": "CavlakBOT",
    "bot": True,
    "timestamp": 1794944528,
    "meta": {"domain": "tr.wiktionary.org", "partition": 0},
}


def test_key_is_wiki_as_utf8_bytes():
    assert build_key(EVENT) == b"trwiktionary"


def test_key_is_none_when_wiki_missing_or_empty():
    assert build_key({"type": "edit"}) is None
    assert build_key({"wiki": ""}) is None


def test_value_round_trips_to_same_event():
    value = build_value(EVENT)
    assert isinstance(value, bytes)
    assert json.loads(value.decode("utf-8")) == EVENT


def test_value_keeps_non_ascii_as_utf8_not_escapes():
    value = build_value(EVENT)
    assert "çekimli".encode("utf-8") in value
    assert b"\\u" not in value


def test_value_is_compact():
    assert b": " not in build_value({"a": 1, "b": [1, 2]})


class FakeMessage:
    def key(self):
        return b"enwiki"

    def topic(self):
        return "recentchange"


def test_delivery_callback_counts_success_and_failure(capsys):
    stats = DeliveryStats()
    stats.on_delivery(None, FakeMessage())
    stats.on_delivery(None, FakeMessage())
    stats.on_delivery("Broker: Message timed out", FakeMessage())
    assert (stats.sent, stats.failed) == (2, 1)
    assert "Message timed out" in capsys.readouterr().err


def test_summary_reports_rate():
    now = iter([0.0, 4.0])
    stats = DeliveryStats(clock=lambda: next(now))
    stats.produced, stats.sent, stats.failed = 10, 8, 1
    assert stats.summary() == "[stats] produced=10 sent=8 failed=1 rate=2.0 msg/s"
