from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
from dataclasses import dataclass

import duckdb
import pandas as pd

from server.models import ColumnInfo, DatasetRegistrationResponse, DatasetSchemaResponse, QueryResponse


@dataclass
class DatasetRecord:
    dataset_id: str
    file_path: str
    table_name: str
    row_count: int
    columns: list[ColumnInfo]


class DatasetRegistry:
    def __init__(self) -> None:
        self._connection = duckdb.connect(database=":memory:")
        self._lock = threading.Lock()
        self._datasets: dict[str, DatasetRecord] = {}

    def count(self) -> int:
        return len(self._datasets)

    def register_dataset(self, dataset_id: str, file_path: str) -> DatasetRegistrationResponse:
        normalized_path, table_override = self._parse_dataset_source(file_path)
        if not os.path.exists(normalized_path):
            raise FileNotFoundError(f"Dataset file not found: {normalized_path}")

        table_name = self._table_name_for(dataset_id)

        with self._lock:
            self._materialize_dataset_source(table_name, normalized_path, table_override)
            row_count = self._connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            columns = self._describe_table(table_name)
            record = DatasetRecord(
                dataset_id=dataset_id,
                file_path=normalized_path,
                table_name=table_name,
                row_count=row_count,
                columns=columns,
            )
            self._datasets[dataset_id] = record

        return DatasetRegistrationResponse(
            dataset_id=record.dataset_id,
            file_path=record.file_path,
            table_name=record.table_name,
            row_count=record.row_count,
            columns=record.columns,
        )

    def get_schema(self, dataset_id: str) -> DatasetSchemaResponse:
        record = self.get_record(dataset_id)
        return DatasetSchemaResponse(dataset_id=record.dataset_id, row_count=record.row_count, columns=record.columns)

    def get_record(self, dataset_id: str) -> DatasetRecord:
        try:
            return self._datasets[dataset_id]
        except KeyError as exc:
            raise KeyError(f"Dataset is not registered: {dataset_id}") from exc

    def execute_sql(self, dataset_id: str, sql_query: str) -> QueryResponse:
        record = self.get_record(dataset_id)
        with self._lock:
            cursor = self._connection.execute(sql_query)
            rows = cursor.fetchall()
            description = cursor.description or []
        columns = [ColumnInfo(name=item[0], type=str(item[1])) for item in description]
        serialized_rows = [list(row) for row in rows]
        return QueryResponse(
            dataset_id=record.dataset_id,
            generated_sql=sql_query,
            row_count=len(serialized_rows),
            columns=columns,
            rows=serialized_rows,
        )

    def execute_user_query(self, dataset_id: str, sql_query: str) -> QueryResponse:
        record = self.get_record(dataset_id)
        with self._lock:
            self._connection.execute('CREATE OR REPLACE TEMP VIEW "dataset" AS SELECT * FROM "' + record.table_name + '"')
            try:
                cursor = self._connection.execute(sql_query)
                rows = cursor.fetchall()
                description = cursor.description or []
            finally:
                self._connection.execute('DROP VIEW IF EXISTS "dataset"')
        columns = [ColumnInfo(name=item[0], type=str(item[1])) for item in description]
        serialized_rows = [list(row) for row in rows]
        return QueryResponse(
            dataset_id=record.dataset_id,
            generated_sql=sql_query,
            row_count=len(serialized_rows),
            columns=columns,
            rows=serialized_rows,
        )

    def _describe_table(self, table_name: str) -> list[ColumnInfo]:
        rows = self._connection.execute(f'DESCRIBE SELECT * FROM "{table_name}"').fetchall()
        return [ColumnInfo(name=row[0], type=row[1]) for row in rows]

    @staticmethod
    def _table_name_for(dataset_id: str) -> str:
        digest = hashlib.sha1(dataset_id.encode("utf-8")).hexdigest()[:12]
        return f"dataset_{digest}"

    @staticmethod
    def _parse_dataset_source(file_path: str) -> tuple[str, str | None]:
        candidate = os.path.abspath(file_path)
        if "::" not in candidate:
            return candidate, None
        base_path, table_name = candidate.rsplit("::", maxsplit=1)
        cleaned_table = table_name.strip()
        return base_path, cleaned_table or None

    def _materialize_dataset_source(self, table_name: str, normalized_path: str, table_override: str | None) -> None:
        extension = os.path.splitext(normalized_path)[1].lower()
        escaped_path = normalized_path.replace("'", "''")

        if extension == ".csv":
            self._connection.execute(
                f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM read_csv_auto('{escaped_path}', HEADER=TRUE)"
            )
            return

        if extension in {".parquet", ".pq"}:
            self._connection.execute(
                f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{escaped_path}')"
            )
            return

        if extension in {".json", ".jsonl", ".ndjson"}:
            self._connection.execute(
                f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM read_json_auto('{escaped_path}')"
            )
            return

        if extension in {".xlsx", ".xls"}:
            frame = pd.read_excel(normalized_path)
            self._register_dataframe_view(table_name, frame)
            return

        if extension in {".db", ".sqlite", ".sqlite3"}:
            frame = self._read_sqlite_frame(normalized_path, table_override)
            self._register_dataframe_view(table_name, frame)
            return

        raise ValueError(
            "Unsupported dataset format. Supported: .csv, .xlsx, .xls, .parquet, .json, .jsonl, .ndjson, .db, .sqlite, .sqlite3"
        )

    def _register_dataframe_view(self, table_name: str, frame: pd.DataFrame) -> None:
        source_name = f"src_{table_name}"
        self._connection.register(source_name, frame)
        try:
            self._connection.execute(f"CREATE OR REPLACE TABLE \"{table_name}\" AS SELECT * FROM \"{source_name}\"")
        finally:
            self._connection.unregister(source_name)

    @staticmethod
    def _read_sqlite_frame(db_path: str, table_override: str | None) -> pd.DataFrame:
        with sqlite3.connect(db_path) as sqlite_conn:
            if table_override:
                table_name = table_override
            else:
                rows = sqlite_conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
                if not rows:
                    raise ValueError("SQLite database has no user tables. Use '<path>::<table_name>' to target a table.")
                table_name = str(rows[0][0])
            escaped_table = table_name.replace('"', '""')
            try:
                return pd.read_sql_query(f'SELECT * FROM "{escaped_table}"', sqlite_conn)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(
                    f"SQLite table not found or unreadable: {table_name}. Use '<path>::<table_name>' with a valid table."
                ) from exc
