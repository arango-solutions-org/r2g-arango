from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from r2g.connectors.csv_source import CsvConnector
from r2g.input.dump_reader import DumpReader


@pytest.fixture
def csv_dir(tmp_path: Path) -> Path:
    (tmp_path / "customers.csv").write_text(
        "id,name,age,is_premium,balance\n"
        "1,Alice,30,true,12.50\n"
        "2,Bob,25,false,0.00\n",
        encoding="utf-8",
    )
    (tmp_path / "orders.csv").write_text(
        "id,customer_id,total\n"
        "10,1,99.99\n"
        "11,2,5.00\n",
        encoding="utf-8",
    )
    (tmp_path / "notes.md").write_text("ignore me", encoding="utf-8")
    return tmp_path


class TestCsvGetSchema:
    def test_discovers_one_table_per_file(self, csv_dir):
        schema = CsvConnector(str(csv_dir)).get_schema()
        assert set(schema.tables) == {"customers", "orders"}

    def test_infers_column_types(self, csv_dir):
        schema = CsvConnector(str(csv_dir)).get_schema()
        cols = {c.name: c.data_type for c in schema.tables["customers"].columns}
        assert cols["id"] == "integer"
        assert cols["name"] == "text"
        assert cols["age"] == "integer"
        assert cols["is_premium"] == "boolean"
        assert cols["balance"] == "double precision"

    def test_id_column_becomes_primary_key(self, csv_dir):
        schema = CsvConnector(str(csv_dir)).get_schema()
        customers = schema.tables["customers"]
        assert customers.primary_key == ["id"]
        id_col = next(c for c in customers.columns if c.name == "id")
        assert id_col.is_primary_key is True
        assert id_col.is_nullable is False

    def test_missing_directory_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="not found"):
            CsvConnector(str(tmp_path / "nope")).get_schema()

    def test_empty_directory_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="No CSV"):
            CsvConnector(str(tmp_path)).get_schema()


class TestCsvSession:
    def test_count_rows(self, csv_dir):
        with CsvConnector(str(csv_dir)).open_session() as sess:
            assert sess.count_rows("customers") == 2
            assert sess.count_rows("orders") == 2

    def test_stream_rows_yields_typed_dicts(self, csv_dir):
        with CsvConnector(str(csv_dir)).open_session() as sess:
            rows = list(sess.stream_rows("customers"))
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["is_premium"] is True

    def test_dump_table_to_csv(self, csv_dir, tmp_path):
        out = tmp_path / "out" / "customers.csv"
        with CsvConnector(str(csv_dir)).open_session() as sess:
            n = sess.dump_table_to_csv("customers", out)
        assert n == 2
        assert out.exists()
        assert "Alice" in out.read_text(encoding="utf-8")

    def test_unknown_table_raises(self, csv_dir):
        with CsvConnector(str(csv_dir)).open_session() as sess:
            with pytest.raises(RuntimeError, match="No CSV file for table"):
                sess.count_rows("missing")


class TestCsvDelimiter:
    def test_tsv_support(self, tmp_path):
        (tmp_path / "t.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
        schema = CsvConnector(str(tmp_path), delimiter="\t").get_schema()
        assert set(c.name for c in schema.tables["t"].columns) == {"a", "b"}


class TestCsvPrimaryKeyHeuristic:
    def test_table_prefixed_id_becomes_pk(self, tmp_path):
        # No bare `id` column; key is `customer_id`.
        (tmp_path / "customers.csv").write_text(
            "customer_id,name\n1,Alice\n2,Bob\n", encoding="utf-8"
        )
        schema = CsvConnector(str(tmp_path)).get_schema()
        t = schema.tables["customers"]
        assert t.primary_key == ["customer_id"]
        col = next(c for c in t.columns if c.name == "customer_id")
        assert col.is_primary_key is True
        assert col.is_nullable is False

    def test_plural_table_singularized_for_key(self, tmp_path):
        # `categories` → singular `category` → `category_id`.
        (tmp_path / "categories.csv").write_text(
            "category_id,label\n1,A\n2,B\n", encoding="utf-8"
        )
        schema = CsvConnector(str(tmp_path)).get_schema()
        assert schema.tables["categories"].primary_key == ["category_id"]

    def test_bare_id_preferred_over_prefixed(self, tmp_path):
        (tmp_path / "customers.csv").write_text(
            "id,customer_id\n1,100\n2,200\n", encoding="utf-8"
        )
        assert CsvConnector(str(tmp_path)).get_schema().tables["customers"].primary_key == ["id"]

    def test_non_unique_candidate_is_not_marked(self, tmp_path):
        # `id` repeats → not a real key, so no PK is detected.
        (tmp_path / "events.csv").write_text(
            "id,kind\n1,a\n1,b\n2,c\n", encoding="utf-8"
        )
        assert CsvConnector(str(tmp_path)).get_schema().tables["events"].primary_key == []

    def test_header_only_file_falls_back_to_name_match(self, tmp_path):
        (tmp_path / "customers.csv").write_text("customer_id,name\n", encoding="utf-8")
        assert (
            CsvConnector(str(tmp_path)).get_schema().tables["customers"].primary_key
            == ["customer_id"]
        )

    def test_no_key_like_column_yields_no_pk(self, tmp_path):
        (tmp_path / "logs.csv").write_text("msg,level\nhi,info\n", encoding="utf-8")
        assert CsvConnector(str(tmp_path)).get_schema().tables["logs"].primary_key == []


class TestCsvBaseDirJail:
    """R2G_CSV_BASE_DIR confines CSV sources to a trusted tree (opt-in)."""

    def test_unset_allows_any_directory(self, csv_dir, monkeypatch):
        monkeypatch.delenv("R2G_CSV_BASE_DIR", raising=False)
        # No jail configured -> behaves exactly as before.
        schema = CsvConnector(str(csv_dir)).get_schema()
        assert "customers" in schema.tables

    def test_inside_base_dir_allowed(self, tmp_path, monkeypatch):
        base = tmp_path / "trusted"
        sub = base / "src"
        sub.mkdir(parents=True)
        (sub / "t.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        monkeypatch.setenv("R2G_CSV_BASE_DIR", str(base))
        schema = CsvConnector(str(sub)).get_schema()
        assert "t" in schema.tables

    def test_base_dir_itself_allowed(self, tmp_path, monkeypatch):
        base = tmp_path / "trusted"
        base.mkdir()
        (base / "t.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        monkeypatch.setenv("R2G_CSV_BASE_DIR", str(base))
        assert "t" in CsvConnector(str(base)).get_schema().tables

    def test_outside_base_dir_rejected(self, tmp_path, monkeypatch):
        from r2g.connectors.csv_source import CsvSourceError

        base = tmp_path / "trusted"
        base.mkdir()
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        monkeypatch.setenv("R2G_CSV_BASE_DIR", str(base))
        with pytest.raises(CsvSourceError, match="outside the allowed base"):
            CsvConnector(str(outside))

    def test_traversal_escape_rejected(self, tmp_path, monkeypatch):
        from r2g.connectors.csv_source import CsvSourceError

        base = tmp_path / "trusted"
        base.mkdir()
        monkeypatch.setenv("R2G_CSV_BASE_DIR", str(base))
        with pytest.raises(CsvSourceError):
            # ../.. climbs out of the jail even though it is written relative to it
            CsvConnector(str(base / ".." / ".." / "etc"))

    def test_session_also_jailed(self, tmp_path, monkeypatch):
        from r2g.connectors.csv_source import CsvSession, CsvSourceError

        base = tmp_path / "trusted"
        base.mkdir()
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        monkeypatch.setenv("R2G_CSV_BASE_DIR", str(base))
        with pytest.raises(CsvSourceError):
            CsvSession(str(outside))


class TestCsvSince:
    """``stream --since`` on a CSV source: the session filters, as the SQL sessions do."""

    @pytest.fixture
    def dated_dir(self, tmp_path: Path) -> Path:
        (tmp_path / "events.csv").write_text(
            "id,last_modified_date,score\n"
            "1,2026-11-01T09:00:00,10\n"
            "2,2026-11-02T09:00:00,20\n"
            "3,2026-11-03T09:00:00,30\n"
            "4,,40\n",
            encoding="utf-8",
        )
        return tmp_path

    def test_without_since_everything_is_read(self, dated_dir):
        with CsvConnector(str(dated_dir)).open_session() as sess:
            assert sess.count_rows("events") == 4
            assert len(list(sess.stream_rows("events"))) == 4

    def test_since_keeps_rows_at_or_after_the_bound(self, dated_dir):
        with CsvConnector(str(dated_dir)).open_session() as sess:
            rows = list(sess.stream_rows(
                "events", since_column="last_modified_date", since_value="2026-11-02T09:00:00",
            ))
            count = sess.count_rows(
                "events", since_column="last_modified_date", since_value="2026-11-02T09:00:00",
            )
        assert [r["id"] for r in rows] == [2, 3]
        assert count == 2

    def test_a_null_never_qualifies(self, dated_dir):
        with CsvConnector(str(dated_dir)).open_session() as sess:
            rows = list(sess.stream_rows(
                "events", since_column="last_modified_date", since_value="2000-01-01T00:00:00",
            ))
        assert [r["id"] for r in rows] == [1, 2, 3]

    def test_numeric_column_compares_as_a_number(self, dated_dir):
        with CsvConnector(str(dated_dir)).open_session() as sess:
            rows = list(sess.stream_rows("events", since_column="score", since_value="30"))
        assert [r["id"] for r in rows] == [3, 4]

    def test_typed_timestamp_column_compares_as_a_timestamp(self, tmp_path):
        import polars as pl

        from r2g.connectors.csv_source import rows_since

        frame = pl.DataFrame({
            "id": [1, 2],
            "ts": ["2026-11-01 09:00:00", "2026-11-02 09:00:00"],
        }).with_columns(pl.col("ts").str.to_datetime())
        kept = rows_since(frame, "ts", "2026-11-02T00:00:00Z")
        assert kept["id"].to_list() == [2]

    def test_missing_since_column_is_an_error_not_a_full_reload(self, dated_dir):
        with CsvConnector(str(dated_dir)).open_session() as sess:
            with pytest.raises(RuntimeError, match="not a column of this CSV table"):
                list(sess.stream_rows("events", since_column="updated_at", since_value="2026-11-02"))
            with pytest.raises(RuntimeError, match="not a column of this CSV table"):
                sess.count_rows("events", since_column="updated_at", since_value="2026-11-02")

    def test_bad_bound_for_the_column_type_is_an_error(self, dated_dir):
        with CsvConnector(str(dated_dir)).open_session() as sess:
            with pytest.raises(RuntimeError, match="is not a number"):
                sess.count_rows("events", since_column="score", since_value="yesterday")

    def test_count_then_stream_reads_the_file_once(self, dated_dir, monkeypatch):
        from r2g.input import dump_reader

        reads: list[str] = []
        real = dump_reader.DumpReader.read_dataframe

        def counting(self):
            reads.append(str(self.file_path))
            return real(self)

        monkeypatch.setattr(dump_reader.DumpReader, "read_dataframe", counting)
        with CsvConnector(str(dated_dir)).open_session() as sess:
            n = sess.count_rows(
                "events", since_column="last_modified_date", since_value="2026-11-02T09:00:00",
            )
            rows = list(sess.stream_rows(
                "events", since_column="last_modified_date", since_value="2026-11-02T09:00:00",
            ))
            # a different bound is a different frame: read again
            again = sess.count_rows("events", since_column="last_modified_date", since_value="2026-11-03")
        assert n == len(rows) == 2 and again == 1
        assert len(reads) == 2


class TestCsvNullMarker:
    """``\\N`` reads as NULL (#73): how PostgreSQL's text ``COPY`` and Snowflake's
    unload write it. Inference, streaming, ``--since`` and the dump all agree."""

    @staticmethod
    def _dirs(tmp_path: Path) -> tuple[Path, Path]:
        dirs = []
        for label, missing in (("marker", "\\N"), ("empty", "")):
            d = tmp_path / label
            d.mkdir()
            (d / "orders.csv").write_text(
                f"ORDER_ID,AMOUNT\n1,10.50\n2,{missing}\n", encoding="utf-8"
            )
            dirs.append(d)
        return dirs[0], dirs[1]

    @staticmethod
    def _read(directory: Path, **kwargs) -> tuple[dict, list]:
        conn = CsvConnector(str(directory), **kwargs)
        types = {c.name: c.data_type for c in conn.get_schema().tables["orders"].columns}
        with conn.open_session() as sess:
            return types, list(sess.stream_rows("orders"))

    def test_the_marker_reads_as_an_empty_field_does(self, tmp_path):
        marker, empty = self._dirs(tmp_path)
        assert self._read(marker) == self._read(empty)
        types, rows = self._read(marker)
        assert types == {"ORDER_ID": "integer", "AMOUNT": "double precision"}
        assert rows[1] == {"ORDER_ID": 2, "AMOUNT": None}

    def test_an_empty_list_keeps_the_marker_as_text(self, tmp_path):
        marker, _ = self._dirs(tmp_path)
        types, rows = self._read(marker, null_values=[])
        assert types["AMOUNT"] == "text"
        assert rows[1]["AMOUNT"] == "\\N"

    def test_a_quoted_empty_string_stays_an_empty_string(self, tmp_path):
        (tmp_path / "notes.csv").write_text('id,note\n1,""\n2,\n3,\\N\n', encoding="utf-8")
        with CsvConnector(str(tmp_path)).open_session() as sess:
            assert [r["note"] for r in sess.stream_rows("notes")] == ["", None, None]

    def test_since_never_selects_a_marker(self, tmp_path):
        (tmp_path / "events.csv").write_text("id,score\n1,10\n2,\\N\n3,30\n", encoding="utf-8")
        with CsvConnector(str(tmp_path)).open_session() as sess:
            rows = list(sess.stream_rows("events", since_column="score", since_value="5"))
            assert [r["id"] for r in rows] == [1, 3]

    def test_the_dump_writes_the_marker_as_an_empty_field(self, tmp_path):
        from r2g.input.dump_reader import DumpReader

        src = tmp_path / "src"
        src.mkdir()
        (src / "orders.csv").write_text(
            'id,amount,code,note\n1,10.50,003,"a, b"\n2,\\N,001,""\n3,7,\\N,\n',
            encoding="utf-8",
        )
        out = tmp_path / "dump" / "orders.csv"
        with CsvConnector(str(src)).open_session() as sess:
            assert sess.dump_table_to_csv("orders", out) == 3
        # Only the markers change: leading zeros, decimal zeros and quoting are kept.
        assert out.read_text(encoding="utf-8") == (
            'id,amount,code,note\n1,10.50,003,"a, b"\n2,,001,""\n3,7,,\n'
        )
        rows = list(DumpReader(str(out)).read_rows())
        assert rows[1]["amount"] is None and rows[2]["code"] is None
        assert rows[1]["note"] == ""

    def test_the_dump_streams_instead_of_reading_the_whole_file(self, tmp_path, monkeypatch):
        # A whole-file read would hold a file larger than memory in one frame.
        def refuse(*_args, **_kwargs):
            raise AssertionError("dump_table_to_csv read the whole file with pl.read_csv")

        src = tmp_path / "src"
        src.mkdir()
        (src / "orders.csv").write_text("1,\\N\n2,9\n", encoding="utf-8")
        out = tmp_path / "dump" / "orders.csv"
        monkeypatch.setattr(pl, "read_csv", refuse)
        with CsvConnector(str(src), has_header=False).open_session() as sess:
            assert sess.dump_table_to_csv("orders", out) == 2
        # Without a header, none is written, and the marker still becomes empty.
        assert out.read_text(encoding="utf-8") == "1,\n2,9\n"

    def test_the_dump_is_a_copy_without_markers(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        body = "id,amount\n1,\\N\n"
        (src / "orders.csv").write_text(body, encoding="utf-8")
        out = tmp_path / "dump" / "orders.csv"
        with CsvConnector(str(src), null_values=[]).open_session() as sess:
            sess.dump_table_to_csv("orders", out)
        assert out.read_text(encoding="utf-8") == body

    def test_the_source_param_reaches_the_connector(self, tmp_path):
        from r2g.connectors.base import create_source_connector

        conn = create_source_connector("csv", str(tmp_path))
        assert conn.null_values == ("\\N",)
        conn = create_source_connector("csv", str(tmp_path), source_params={"null_values": "NULL"})
        assert conn.null_values == ("NULL",)
        conn = create_source_connector("csv", str(tmp_path), source_params={"null_values": []})
        assert conn.null_values == ()
        with pytest.raises(ValueError, match="null_values"):
            create_source_connector("csv", str(tmp_path), source_params={"null_values": 0})


def _late_text_file(directory: Path, text_row: int, value: str = "ABC", total: int = 3000) -> None:
    """A table whose CODE column holds whole numbers except one value at ``text_row``."""
    lines = ["ID,CODE,NAME"]
    for i in range(1, total + 1):
        lines.append(f"{i},{value if i == text_row else i % 97},name{i}")
    (directory / "events.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestCsvTypesComeFromTheWholeFile:
    """A value anywhere in the file decides its column's type, and never fails a read.

    The snapshot used to type a column from its first 1,000 rows. Polars parses a
    little past ``n_rows`` with the types it guessed, so text just after row 1,000
    in a column of whole numbers failed the snapshot; further down it was missed
    and the column was recorded as a number. The load guessed from 100 rows and
    failed on any later text.
    """

    @pytest.mark.parametrize("value", ["ABC", "N/A"])
    @pytest.mark.parametrize("text_row", [150, 1000, 1001, 1200, 2000, 2900])
    def test_one_text_value_anywhere_makes_the_column_text(self, tmp_path, text_row, value):
        _late_text_file(tmp_path, text_row, value)
        table = CsvConnector(str(tmp_path)).get_schema().tables["events"]
        types = {c.name: c.data_type for c in table.columns}
        assert types == {"ID": "integer", "CODE": "text", "NAME": "text"}
        assert table.primary_key == ["ID"]

    def test_one_decimal_anywhere_makes_the_column_floating_point(self, tmp_path):
        _late_text_file(tmp_path, 2900, "12.5")
        table = CsvConnector(str(tmp_path)).get_schema().tables["events"]
        assert {c.name: c.data_type for c in table.columns}["CODE"] == "double precision"

    @pytest.mark.parametrize("text_row", [150, 1200, 2900])
    def test_the_load_reads_every_row_whatever_the_first_rows_look_like(
        self, tmp_path, text_row
    ):
        _late_text_file(tmp_path, text_row)
        with CsvConnector(str(tmp_path)).open_session() as sess:
            rows = list(sess.stream_rows("events"))
        assert len(rows) == 3000
        assert rows[text_row - 1]["CODE"] == "ABC"
        # One text value makes the column text in the file as a whole.
        assert rows[0]["CODE"] == "1"
        assert rows[0]["ID"] == 1

    @pytest.mark.parametrize("value", ["ABC", "12.5", ""])
    @pytest.mark.parametrize("text_row", [150, 1200, 2900])
    def test_the_snapshot_types_a_column_as_the_load_reads_it(self, tmp_path, text_row, value):
        from r2g.connectors.csv_source import _polars_dtype_to_type

        _late_text_file(tmp_path, text_row, value)
        conn = CsvConnector(str(tmp_path))
        table = conn.get_schema().tables["events"]
        with conn.open_session() as sess:
            loaded = sess._reader("events").read_dataframe()
        assert [c.data_type for c in table.columns] == [
            _polars_dtype_to_type(d) for d in loaded.dtypes
        ]

    def test_a_column_empty_at_the_top_is_typed_by_the_values_below(self, tmp_path):
        lines = ["id,qty"] + [f"{i},{'' if i <= 150 else i}" for i in range(1, 301)]
        (tmp_path / "t.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        conn = CsvConnector(str(tmp_path))
        assert [c.data_type for c in conn.get_schema().tables["t"].columns] == [
            "integer",
            "integer",
        ]
        with conn.open_session() as sess:
            rows = list(sess.stream_rows("t"))
        assert rows[0]["qty"] is None and rows[150]["qty"] == 151

    def test_types_are_what_polars_infers_for_the_same_file(self, tmp_path):
        body = (
            "id,name,qty,price,active,seen_at,note,zip\n"
            '1,Alice,3,12.50,true,2026-01-02T03:04:05,"",00789\n'
            "2,Bob,,0.00,false,2026-02-03T04:05:06,x,10001\n"
            "3,\\N,7,\\N,true,\\N,y,\\N\n"
        )
        (tmp_path / "t.csv").write_text(body, encoding="utf-8")
        direct = pl.read_csv(
            str(tmp_path / "t.csv"), infer_schema_length=None, null_values=["\\N"]
        )
        table = CsvConnector(str(tmp_path)).get_schema().tables["t"]
        from r2g.connectors.csv_source import _polars_dtype_to_type

        assert [c.data_type for c in table.columns] == [
            _polars_dtype_to_type(d) for d in direct.dtypes
        ]

    def test_a_headerless_tab_separated_file_is_typed_too(self, tmp_path):
        (tmp_path / "t.tsv").write_text("1\ta,b\t2.5\n2\tc\t3.5\n", encoding="utf-8")
        table = CsvConnector(str(tmp_path), delimiter="\t", has_header=False).get_schema().tables["t"]
        assert [(c.name, c.data_type) for c in table.columns] == [
            ("column_1", "integer"),
            ("column_2", "text"),
            ("column_3", "double precision"),
        ]


def _keyed_file(directory: Path, bad_row: int | None, bad_id: str, total: int = 3000) -> None:
    """``events.csv`` whose ID column is 1..total, except ``bad_id`` at ``bad_row``."""
    lines = ["ID,CODE"]
    for i in range(1, total + 1):
        lines.append(f"{bad_id if i == bad_row else i},{i % 97}")
    (directory / "events.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestCsvKeyCheckReadsTheWholeFile:
    """A column is proposed as the key only when it is unique and non-null in every row.

    The check used to read the first 1,000 rows. A value repeated once is seen in
    a sample only when both copies fall inside it, so a non-unique column was
    proposed as the key for almost any file longer than the sample.
    """

    def test_a_unique_column_is_the_key(self, tmp_path):
        _keyed_file(tmp_path, None, "")
        table = CsvConnector(str(tmp_path)).get_schema().tables["events"]
        assert table.primary_key == ["ID"]
        assert [c.is_primary_key for c in table.columns] == [True, False]
        assert [c.is_nullable for c in table.columns] == [False, True]

    @pytest.mark.parametrize("bad_row", [2, 999, 1001, 1500, 3000])
    def test_one_repeated_value_anywhere_means_no_key(self, tmp_path, bad_row):
        _keyed_file(tmp_path, bad_row, "1")
        table = CsvConnector(str(tmp_path)).get_schema().tables["events"]
        assert table.primary_key == []
        assert not any(c.is_primary_key for c in table.columns)

    @pytest.mark.parametrize("bad_row", [2, 1001, 3000])
    def test_one_missing_value_anywhere_means_no_key(self, tmp_path, bad_row):
        _keyed_file(tmp_path, bad_row, "")
        assert CsvConnector(str(tmp_path)).get_schema().tables["events"].primary_key == []

    def test_a_null_marker_counts_as_missing(self, tmp_path):
        _keyed_file(tmp_path, 2500, "\\N")
        table = CsvConnector(str(tmp_path)).get_schema().tables["events"]
        assert table.primary_key == []
        assert {c.name: c.data_type for c in table.columns}["ID"] == "integer"

    def test_the_next_candidate_is_tried_when_the_first_repeats(self, tmp_path):
        lines = ["id,event_id"] + [f"{1 if i == 2000 else i},{i}" for i in range(1, 3001)]
        (tmp_path / "events.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        assert CsvConnector(str(tmp_path)).get_schema().tables["events"].primary_key == ["event_id"]

    def test_a_headerless_file_has_no_named_key(self, tmp_path):
        (tmp_path / "events.csv").write_text("1,a\n2,b\n", encoding="utf-8")
        assert CsvConnector(str(tmp_path), has_header=False).get_schema().tables[
            "events"
        ].primary_key == []

    @pytest.mark.parametrize("header", [b"ID,NAME,V\n", b"A,NAME,V\n"], ids=["id", "no-id"])
    @pytest.mark.parametrize(
        "bad_line",
        [b"%d,caf\xe9,1.5\n", b'%d,"open,1.5\n', b"%d,name,1.5,x\n"],
        ids=["invalid-utf8", "unclosed-quote", "extra-field"],
    )
    def test_a_file_the_load_cannot_read_fails_the_snapshot(self, tmp_path, header, bad_line):
        # With or without a key candidate: the snapshot and the load agree.
        body = header + b"".join(
            (bad_line if i == 2000 else b"%d,name,1.5\n") % i for i in range(1, 3001)
        )
        path = tmp_path / "events.csv"
        path.write_bytes(body)
        with pytest.raises(pl.exceptions.PolarsError):
            DumpReader(str(path)).read_dataframe()
        with pytest.raises(RuntimeError, match="for 'events'"):
            CsvConnector(str(tmp_path)).get_schema()

    @pytest.mark.parametrize("header", ["ID,NAME,V\n", "A,NAME,V\n"], ids=["id", "no-id"])
    def test_a_short_row_is_read_by_both(self, tmp_path, header):
        body = header + "".join(
            f"{i},name\n" if i == 2000 else f"{i},name,1.5\n" for i in range(1, 3001)
        )
        path = tmp_path / "events.csv"
        path.write_text(body, encoding="utf-8")
        assert DumpReader(str(path)).read_dataframe().height == 3000
        table = CsvConnector(str(tmp_path)).get_schema().tables["events"]
        assert table.primary_key == (["ID"] if header.startswith("ID") else [])
