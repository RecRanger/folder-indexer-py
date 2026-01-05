from pathlib import Path

import polars as pl

from folder_indexer.indexer import run_file_indexer_and_merge


def test_with_single_file(tmp_path: Path) -> None:
    input_folder = tmp_path / "input"
    input_folder.mkdir()
    (input_folder / "sample.txt").write_text("This is a sample file.")

    run_file_indexer_and_merge(
        input_folder=input_folder,
        output_folder=tmp_path,
        strip_prefix=True,
    )

    assert (tmp_path / "file_index.parquet").is_file()

    df = pl.read_parquet(tmp_path / "file_index.parquet")
    assert df.shape == (1, 13)
