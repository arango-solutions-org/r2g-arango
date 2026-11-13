from __future__ import annotations

import polars as pl
import pytest

from r2g.input.dump_reader import DumpReader


class TestReadRows:
    def test_yields_correct_dicts_from_csv(self, sample_csv):
        reader = DumpReader(sample_csv)
        rows = list(reader.read_rows())

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == "alice@example.com"
        assert rows[4]["name"] == "Eve"

    def test_all_rows_have_expected_keys(self, sample_csv):
        reader = DumpReader(sample_csv)
        rows = list(reader.read_rows())
        for row in rows:
            assert set(row.keys()) == {"id", "name", "email", "age", "active"}

    def test_reads_tsv_with_tab_delimiter(self, sample_tsv):
        reader = DumpReader(sample_tsv, delimiter="\t")
        rows = list(reader.read_rows())

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[2]["name"] == "Carol"


class TestReadDataframe:
    def test_returns_polars_dataframe(self, sample_csv):
        reader = DumpReader(sample_csv)
        df = reader.read_dataframe()

        assert isinstance(df, pl.DataFrame)

    def test_correct_shape(self, sample_csv):
        reader = DumpReader(sample_csv)
        df = reader.read_dataframe()

        assert df.shape == (5, 5)

    def test_column_names(self, sample_csv):
        reader = DumpReader(sample_csv)
        df = reader.read_dataframe()

        assert df.columns == ["id", "name", "email", "age", "active"]


class TestRowCount:
    def test_returns_five(self, sample_csv):
        reader = DumpReader(sample_csv)
        assert reader.row_count() == 5

    def test_tsv_returns_five(self, sample_tsv):
        reader = DumpReader(sample_tsv, delimiter="\t")
        assert reader.row_count() == 5


class TestReadChunks:
    def test_returns_batches(self, sample_csv):
        reader = DumpReader(sample_csv)
        chunks = list(reader.read_chunks(chunk_size=2))

        assert len(chunks) >= 1
        total_rows = sum(chunk.shape[0] for chunk in chunks)
        assert total_rows == 5

    def test_each_chunk_is_dataframe(self, sample_csv):
        reader = DumpReader(sample_csv)
        for chunk in reader.read_chunks(chunk_size=3):
            assert isinstance(chunk, pl.DataFrame)


class TestFileNotFound:
    def test_read_rows_raises(self, tmp_path):
        reader = DumpReader(str(tmp_path / "nope.csv"))
        with pytest.raises(FileNotFoundError):
            list(reader.read_rows())

    def test_read_dataframe_raises(self, tmp_path):
        reader = DumpReader(str(tmp_path / "nope.csv"))
        with pytest.raises(FileNotFoundError):
            reader.read_dataframe()

    def test_row_count_raises(self, tmp_path):
        reader = DumpReader(str(tmp_path / "nope.csv"))
        with pytest.raises(FileNotFoundError):
            reader.row_count()

    def test_read_chunks_raises(self, tmp_path):
        reader = DumpReader(str(tmp_path / "nope.csv"))
        with pytest.raises(FileNotFoundError):
            list(reader.read_chunks(chunk_size=2))


class TestTypesComeFromTheWholeFile:
    """Polars' default types a column from its first 100 rows and then fails on a
    later value that does not fit; the reader types from every row instead."""

    @pytest.fixture
    def late_text(self, tmp_path):
        lines = ["id,code"] + [f"{i},{'ABC' if i == 150 else i}" for i in range(1, 301)]
        path = tmp_path / "late.csv"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(path)

    def test_read_dataframe(self, late_text):
        df = DumpReader(late_text).read_dataframe()
        assert df.height == 300
        assert df.schema["id"] == pl.Int64
        assert df["code"][149] == "ABC"

    def test_read_rows(self, late_text):
        rows = list(DumpReader(late_text).read_rows())
        assert rows[149] == {"id": 150, "code": "ABC"}

    def test_read_chunks(self, late_text):
        chunks = list(DumpReader(late_text).read_chunks(chunk_size=100))
        assert sum(c.height for c in chunks) == 300
        assert "ABC" in pl.concat(chunks)["code"].to_list()

    def test_row_count(self, late_text):
        assert DumpReader(late_text).row_count() == 300
