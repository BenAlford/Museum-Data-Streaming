"""Unit tests for phase2_etl_pipeline.py and the functions it imports."""
# pylint: skip-file
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from psycopg2 import sql

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "pipeline"))

# phase2_etl_pipeline reads DB settings and connects on import, so stub both first.
DB_ENV = {"DB_HOST": "host", "DB_NAME": "db",
          "DB_USERNAME": "user", "DB_PASSWORD": "pass"}
with patch("psycopg2.connect") as _mock_connect, patch.dict("os.environ", DB_ENV):
    _mock_cursor = _mock_connect.return_value.cursor.return_value.__enter__.return_value
    _mock_cursor.fetchall.return_value = []
    import read_kafka as kafka
    import etl_pipeline as etl
    import transform_data as pipe

VALID_TIME = "2024-01-01T10:00:00"


def make_message(payload, error=None):
    """Build a mock Kafka message whose value is the JSON-encoded payload."""
    raw = payload if isinstance(payload, bytes) else json.dumps(
        payload).encode("utf-8")
    msg = MagicMock()
    msg.value.return_value = raw
    msg.error.return_value = error
    return msg


@pytest.fixture
def exhibition_data():
    """Mock master data for the exhibition table."""
    return pd.DataFrame({
        "exhibition_id": [0, 1, 2],
        "exhibition_name": ["Zero", "One", "Two"],
    })


@pytest.fixture
def ratings_data():
    """Mock master data for the rating table."""
    return pd.DataFrame({
        "rating_id": [10, 11, 12, 13, 14],
        "rating_value": [0, 1, 2, 3, 4],
    })


@pytest.fixture
def requests_data():
    """Mock master data for the request table."""
    return pd.DataFrame({
        "request_id": [20, 21],
        "request_value": [0, 1],
    })


@pytest.fixture
def sequential_pool():
    """Replace multiprocessing.Pool, which can't pickle mock messages, with an in-process map."""
    with patch.object(etl, "Pool") as mock_pool:
        pool = mock_pool.return_value.__enter__.return_value
        pool.map.side_effect = lambda fn, items: [fn(i) for i in items]
        yield mock_pool


class TestValidateDatetime:
    """Tests for phase1_read_kafka.validate_datetime."""

    @pytest.mark.parametrize("time_str", [
        "2024-01-01T08:45:00",
        "2024-01-01T12:00:00",
        "2024-01-01T18:15:00",
    ])
    def test_valid_times_within_opening_hours(self, time_str):
        assert kafka.validate_datetime(datetime.fromisoformat(time_str))

    @pytest.mark.parametrize("time_str", [
        "2024-01-01T08:44:59",
        "2024-01-01T18:15:01",
        "2024-01-01T00:00:00",
        "2024-01-01T23:59:59",
    ])
    def test_times_outside_opening_hours(self, time_str, caplog):
        with caplog.at_level(logging.INFO):
            assert not kafka.validate_datetime(
                datetime.fromisoformat(time_str))
        assert "outside opening hours" in caplog.text

    def test_future_naive_datetime(self, caplog):
        future = datetime.now() + timedelta(days=1)
        with caplog.at_level(logging.INFO):
            assert not kafka.validate_datetime(future)
        assert "in the future" in caplog.text

    def test_future_timezone_aware_datetime(self):
        future = datetime.now(timezone.utc) + timedelta(days=1)
        assert not kafka.validate_datetime(future)


class TestValidateMessage:
    """Tests for phase1_read_kafka.validate_message."""

    @pytest.mark.parametrize("payload", [
        {"at": VALID_TIME, "site": 1, "val": 3},
        {"at": VALID_TIME, "site": "2", "val": 0},
        {"at": VALID_TIME, "site": 0, "val": 4},
        {"at": VALID_TIME, "site": 5, "val": -1, "type": 0},
        {"at": VALID_TIME, "site": 5, "val": -1, "type": 1},
    ])
    def test_valid_messages(self, payload):
        assert kafka.validate_message(make_message(payload))

    @pytest.mark.parametrize("payload", [
        ["not", "a", "dict"],
        "a string",
        {"site": 1, "val": 3},
        {"at": VALID_TIME, "val": 3},
        {"at": VALID_TIME, "site": 1},
    ])
    def test_malformed_structure(self, payload):
        assert not kafka.validate_message(make_message(payload))

    @pytest.mark.parametrize("val", [5, -2, "3", 2.5, None])
    def test_invalid_val(self, val):
        payload = {"at": VALID_TIME, "site": 1, "val": val}
        assert not kafka.validate_message(make_message(payload))

    @pytest.mark.parametrize("extra", [{}, {"type": 2}, {"type": "0"}])
    def test_request_requires_valid_type(self, extra):
        payload = {"at": VALID_TIME, "site": 1, "val": -1, **extra}
        assert not kafka.validate_message(make_message(payload))

    @pytest.mark.parametrize("site", ["abc", "-1", "6", 6, -1, 1.5, None, [1]])
    def test_invalid_site(self, site):
        payload = {"at": VALID_TIME, "site": site, "val": 3}
        assert not kafka.validate_message(make_message(payload))

    @pytest.mark.parametrize("at", [
        "2024-01-01T07:00:00",
        "2024-01-01T19:00:00",
        (datetime.now() + timedelta(days=1)).isoformat(),
    ])
    def test_invalid_datetime(self, at):
        payload = {"at": at, "site": 1, "val": 3}
        assert not kafka.validate_message(make_message(payload))

    def test_invalid_json(self, caplog):
        with caplog.at_level(logging.ERROR):
            assert not kafka.validate_message(make_message(b"{not json"))
        assert "Failed to decode message" in caplog.text

    def test_message_without_value(self):
        msg = None
        assert not kafka.validate_message(msg)


class TestConsumeLoop:
    """Tests for phase1_read_kafka.consume_loop."""

    @staticmethod
    def run_loop(consumer, transform_fn=None):
        """Run the loop for as many iterations as there are polled messages; returns the batch."""
        stop_event = MagicMock()
        stop_event.is_set.side_effect = [
            False] * consumer.message_count + [True]
        batch = []
        kafka.consume_loop(consumer, ["lmnh"], Lock(), batch,
                           stop_event, transform_fn=transform_fn)
        return batch

    @staticmethod
    def make_consumer(messages):
        consumer = MagicMock()
        consumer.poll.side_effect = messages
        consumer.message_count = len(messages)
        return consumer

    def test_subscribes_and_closes_consumer(self):
        consumer = self.make_consumer([None])
        self.run_loop(consumer)
        consumer.subscribe.assert_called_once_with(["lmnh"])
        consumer.close.assert_called_once()

    def test_valid_message_is_added_to_batch(self):
        payload = {"at": VALID_TIME, "site": "1", "val": 3}
        msg = make_message(payload)
        consumer = self.make_consumer([msg])
        transform_fn = MagicMock()

        batch = self.run_loop(consumer, transform_fn)

        assert batch == [msg]
        transform_fn.assert_not_called()

    def test_valid_message_without_transform_fn(self):
        payload = {"at": VALID_TIME, "site": "1", "val": 3}
        msg = make_message(payload)
        consumer = self.make_consumer([msg])
        assert self.run_loop(consumer) == [msg]

    def test_error_message_is_skipped_and_logged(self, caplog):
        consumer = self.make_consumer(
            [make_message({"at": VALID_TIME, "site": 1, "val": 3}, error="boom")])

        with caplog.at_level(logging.ERROR):
            batch = self.run_loop(consumer, MagicMock())

        assert batch == []
        assert "Kafka error" in caplog.text

    def test_none_messages_are_ignored(self):
        consumer = self.make_consumer([None, None])
        batch = self.run_loop(consumer, MagicMock())
        assert batch == []
        assert consumer.poll.call_count == 2

    def test_consumer_closed_when_error_raised(self):
        consumer = MagicMock()
        consumer.subscribe.side_effect = RuntimeError("fail")
        with pytest.raises(RuntimeError):
            kafka.consume_loop(consumer, ["lmnh"], Lock(), [], MagicMock())
        consumer.close.assert_called_once()


class TestGetValidKafkaMessages:
    """Tests for phase2_etl_pipeline.get_valid_kafka_messages."""

    def test_invalid_message_is_skipped_and_logged(self, sequential_pool, caplog):
        msg = make_message({"at": VALID_TIME, "site": 99, "val": 3})

        with caplog.at_level(logging.INFO):
            rows = etl.get_valid_kafka_messages([msg])

        assert rows == []
        assert "Site out of range" in caplog.text


class TestGetDataFromMuseumTable:
    """Tests for pipeline.get_data_from_museum_table."""

    @staticmethod
    def make_connection(rows):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = rows
        return connection, cursor

    def test_returns_dataframe_of_rows(self):
        rows = [{"exhibition_id": 1, "name": "A"},
                {"exhibition_id": 2, "name": "B"}]
        connection, _ = self.make_connection(rows)

        result = pipe.get_data_from_museum_table("exhibition", connection)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict("records") == rows

    def test_empty_table_returns_empty_dataframe(self):
        connection, _ = self.make_connection([])
        assert pipe.get_data_from_museum_table("rating", connection).empty

    def test_query_uses_quoted_table_identifier(self):
        connection, cursor = self.make_connection([])

        pipe.get_data_from_museum_table("request", connection)

        query = cursor.execute.call_args.args[0]
        assert isinstance(query, sql.Composed)
        identifiers = [part for part in query.seq
                       if isinstance(part, sql.Identifier)]
        assert [i.strings for i in identifiers] == [("request",)]


class TestTransformExhibitionRatingsData:
    """Tests for pipeline.transform_exhibition_ratings_data."""

    def test_returns_expected_columns_and_values(self, ratings_data):
        df = pd.DataFrame({
            "at": ["2024-01-01T10:00:00", "2024-01-01T11:00:00"],
            "exhibition_id": [1, 2],
            "val": [3, 0],
        })

        result = pipe.transform_exhibition_ratings_data(df, ratings_data)

        assert list(result.columns) == [
            "time_at", "exhibition_id", "rating_id"]
        assert result["rating_id"].tolist() == [13, 10]
        assert result["exhibition_id"].tolist() == [1, 2]
        assert pd.api.types.is_datetime64_any_dtype(result["time_at"])
        assert result["time_at"].iloc[0] == pd.Timestamp("2024-01-01 10:00:00")

    def test_drops_values_without_matching_rating(self, ratings_data):
        df = pd.DataFrame({
            "at": ["2024-01-01T10:00:00", "2024-01-01T10:05:00"],
            "exhibition_id": [1, 1],
            "val": [2, 99],
        })

        result = pipe.transform_exhibition_ratings_data(df, ratings_data)

        assert result["rating_id"].tolist() == [12]

    def test_removes_duplicates(self, ratings_data):
        df = pd.DataFrame({
            "at": ["2024-01-01T10:00:00"] * 2,
            "exhibition_id": [1, 1],
            "val": [2, 2],
        })

        result = pipe.transform_exhibition_ratings_data(df, ratings_data)

        assert len(result) == 1

    def test_empty_input_gives_empty_output(self, ratings_data):
        df = pd.DataFrame({
            "at": pd.Series([], dtype="object"),
            "exhibition_id": pd.Series([], dtype="int64"),
            "val": pd.Series([], dtype="int64"),
        })

        result = pipe.transform_exhibition_ratings_data(df, ratings_data)

        assert result.empty
        assert list(result.columns) == [
            "time_at", "exhibition_id", "rating_id"]


class TestTransformExhibitionRequestsData:
    """Tests for pipeline.transform_exhibition_requests_data."""

    def test_returns_expected_columns_and_values(self, requests_data):
        df = pd.DataFrame({
            "at": ["2024-01-01T10:00:00", "2024-01-01T11:00:00"],
            "exhibition_id": [1, 2],
            "type": [0, 1],
        })

        result = pipe.transform_exhibition_requests_data(df, requests_data)

        assert list(result.columns) == [
            "time_at", "exhibition_id", "request_id"]
        assert result["request_id"].tolist() == [20, 21]
        assert result["exhibition_id"].tolist() == [1, 2]
        assert pd.api.types.is_datetime64_any_dtype(result["time_at"])

    def test_drops_types_without_matching_request(self, requests_data):
        df = pd.DataFrame({
            "at": ["2024-01-01T10:00:00", "2024-01-01T10:05:00"],
            "exhibition_id": [1, 1],
            "type": [1, 7],
        })

        result = pipe.transform_exhibition_requests_data(df, requests_data)

        assert result["request_id"].tolist() == [21]

    def test_removes_duplicates(self, requests_data):
        df = pd.DataFrame({
            "at": ["2024-01-01T10:00:00"] * 2,
            "exhibition_id": [1, 1],
            "type": [0, 0],
        })

        result = pipe.transform_exhibition_requests_data(df, requests_data)

        assert len(result) == 1

    def test_empty_input_gives_empty_output(self, requests_data):
        df = pd.DataFrame({
            "at": pd.Series([], dtype="object"),
            "exhibition_id": pd.Series([], dtype="int64"),
            "type": pd.Series([], dtype="int64"),
        })

        result = pipe.transform_exhibition_requests_data(df, requests_data)

        assert result.empty
        assert list(result.columns) == [
            "time_at", "exhibition_id", "request_id"]


class TestTransformData:
    """Tests for phase2_etl_pipeline.transform_data."""

    @pytest.fixture(autouse=True)
    def patch_globals(self, monkeypatch, sequential_pool, exhibition_data, ratings_data,
                      requests_data):
        monkeypatch.setattr(etl, "EXHIBITION_DATA", exhibition_data)
        monkeypatch.setattr(etl, "EXHIBITION_RATINGS_DATA", ratings_data)
        monkeypatch.setattr(etl, "EXHIBITION_REQUESTS_DATA", requests_data)
        self.load = MagicMock()
        monkeypatch.setattr(etl, "load", self.load)

    def loaded_rows(self):
        """Return the (ratings, requests) rows passed to load as lists of tuples."""
        ratings_df, requests_df, _ = self.load.call_args.args
        return (list(ratings_df.itertuples(index=False, name=None)),
                list(requests_df.itertuples(index=False, name=None)))

    def test_rating_row_is_loaded_as_rating(self):
        etl.transform_data(
            [make_message({"at": VALID_TIME, "site": "1", "val": 3})])

        ratings, requests = self.loaded_rows()
        assert ratings == [(pd.Timestamp(VALID_TIME), 1, 13)]
        assert requests == []

    def test_request_row_is_loaded_as_request(self):
        etl.transform_data(
            [make_message({"at": VALID_TIME, "site": "2", "val": -1, "type": 1})])

        ratings, requests = self.loaded_rows()
        assert ratings == []
        assert requests == [(pd.Timestamp(VALID_TIME), 2, 21)]

    def test_unknown_site_is_logged_and_not_loaded(self, caplog):
        # Site 4 passes validation but has no exhibition row.
        with caplog.at_level(logging.ERROR):
            etl.transform_data(
                [make_message({"at": VALID_TIME, "site": "4", "val": 3})])

        assert "Total rows lost" in caplog.text
        assert self.loaded_rows() == ([], [])

    def test_rating_without_matching_value_is_not_loaded(self, monkeypatch, ratings_data):
        monkeypatch.setattr(etl, "EXHIBITION_RATINGS_DATA",
                            ratings_data[ratings_data["rating_value"] != 4])

        etl.transform_data(
            [make_message({"at": VALID_TIME, "site": "1", "val": 4})])

        assert self.loaded_rows() == ([], [])

    def test_request_without_matching_type_is_not_loaded(self, monkeypatch, requests_data):
        monkeypatch.setattr(etl, "EXHIBITION_REQUESTS_DATA",
                            requests_data[requests_data["request_value"] != 1])

        etl.transform_data(
            [make_message({"at": VALID_TIME, "site": "1", "val": -1, "type": 1})])

        assert self.loaded_rows() == ([], [])


class TestValidateMessageRobustness:
    """validate_message should reject bad input by returning False, never by raising."""

    @pytest.mark.parametrize("at", ["not a date", "", None, 12345, "2024-13-45T10:00:00"])
    def test_invalid_at_value_is_rejected(self, at):
        payload = {"at": at, "site": 1, "val": 3}
        assert kafka.validate_message(make_message(payload)) is False

    def test_non_utf8_bytes_are_rejected(self):
        assert kafka.validate_message(make_message(b"\xff\xfe\x00")) is False

    @pytest.mark.parametrize("val", [True, False])
    def test_boolean_val_is_rejected(self, val):
        payload = {"at": VALID_TIME, "site": 1, "val": val}
        assert kafka.validate_message(make_message(payload)) is False

    @pytest.mark.parametrize("site", [True, False])
    def test_boolean_site_is_rejected(self, site):
        payload = {"at": VALID_TIME, "site": site, "val": 3}
        assert kafka.validate_message(make_message(payload)) is False

    def test_boolean_request_type_is_rejected(self):
        payload = {"at": VALID_TIME, "site": 1, "val": -1, "type": True}
        assert kafka.validate_message(make_message(payload)) is False

    @pytest.mark.parametrize("site", ["½", "²", "٣٣"])
    def test_non_ascii_numeric_site_is_rejected(self, site):
        payload = {"at": VALID_TIME, "site": site, "val": 3}
        assert kafka.validate_message(make_message(payload)) is False


class TestConsumeLoopResilience:
    """A single bad message must never stop the consumer from handling later ones."""

    VALID_PAYLOAD = {"at": VALID_TIME, "site": "1", "val": 3}

    @pytest.mark.parametrize("bad_message", [
        make_message({"at": "garbage", "site": 1, "val": 3}),
        make_message({"at": None, "site": 1, "val": 3}),
        make_message(b"\xff\xfe\x00"),
        make_message(b"{not json"),
        make_message(["not", "a", "dict"]),
    ])
    def test_bad_message_does_not_stop_loop(self, bad_message, sequential_pool):
        consumer = TestConsumeLoop.make_consumer(
            [bad_message, make_message(self.VALID_PAYLOAD)])

        batch = TestConsumeLoop.run_loop(consumer)
        rows = etl.get_valid_kafka_messages(batch)

        assert rows == [{**self.VALID_PAYLOAD, "site": 1}]
        consumer.close.assert_called_once()

    def test_invalid_message_is_never_transformed(self, sequential_pool, monkeypatch):
        mock_transform = MagicMock()
        mock_load = MagicMock()
        monkeypatch.setattr(etl, "transform", mock_transform)
        monkeypatch.setattr(etl, "load", mock_load)

        etl.transform_data(
            [make_message({"at": VALID_TIME, "site": 1, "val": 9})])

        mock_transform.assert_not_called()
        mock_load.assert_not_called()


class TestValidateSite:
    """Tests for phase1_read_kafka.validate_site."""

    @pytest.mark.parametrize("site", [0, 1, 2, 3, 4, 5, "0", "1", "2", "3", "4", "5"])
    def test_valid_sites(self, site):
        assert kafka.validate_site(site) is True

    @pytest.mark.parametrize("site", [-1, 6, 100, "6", "10", "99"])
    def test_out_of_range_sites(self, site, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_site(site) is False
        assert "Site out of range" in caplog.text

    @pytest.mark.parametrize("site", ["abc", "", " ", "-1", "1.5", " 1", "1 ", "+1"])
    def test_non_numeric_strings(self, site, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_site(site) is False
        assert "Unable to convert site to int" in caplog.text

    @pytest.mark.parametrize("site", [1.0, 1.5, None, [1], {"site": 1}, (1,)])
    def test_non_int_types(self, site, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_site(site) is False
        assert "Invalid site type" in caplog.text

    @pytest.mark.parametrize("site", [True, False])
    def test_booleans_are_rejected(self, site, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_site(site) is False
        assert "Invalid site in message" in caplog.text

    @pytest.mark.parametrize("site", ["½", "²"])
    def test_non_ascii_numeric_strings_are_rejected_not_raised(self, site):
        assert kafka.validate_site(site) is False


class TestValidateValAndType:
    """Tests for phase1_read_kafka.validate_val_and_type."""

    @pytest.mark.parametrize("val", [0, 1, 2, 3, 4])
    def test_valid_ratings(self, val):
        assert kafka.validate_val_and_type(val, None) is True

    @pytest.mark.parametrize("type_field", [0, 1])
    def test_valid_requests(self, type_field):
        assert kafka.validate_val_and_type(-1, type_field) is True

    @pytest.mark.parametrize("type_field", [None, 2, "garbage", True, [0]])
    def test_type_is_ignored_for_ratings(self, type_field):
        assert kafka.validate_val_and_type(3, type_field) is True

    @pytest.mark.parametrize("val", [-2, -100, 5, 100])
    def test_out_of_range_val(self, val, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_val_and_type(val, 0) is False
        assert "Invalid val in message" in caplog.text

    @pytest.mark.parametrize("val", ["3", "-1", 2.5, 3.0, None, [1], {"val": 1}])
    def test_non_int_val(self, val, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_val_and_type(val, 0) is False
        assert "Invalid val in message" in caplog.text

    @pytest.mark.parametrize("val", [True, False])
    def test_boolean_val_is_rejected(self, val, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_val_and_type(val, 0) is False
        assert "Invalid val in message" in caplog.text

    @pytest.mark.parametrize("type_field", [None, 2, -1, "0", "1", [0], True, False])
    def test_request_with_invalid_type(self, type_field, caplog):
        with caplog.at_level(logging.INFO):
            assert kafka.validate_val_and_type(-1, type_field) is False
        assert "Invalid type field for val -1" in caplog.text
