"""
SQLite database implementation for Alarm News System.

Lightweight alternative to MongoDB for local development and small deployments.
Implements the same DatabaseInterface, so all existing code works without changes.

Features:
- Zero configuration (file-based, no server needed)
- Python built-in (no extra dependencies)
- JSON storage for flexible document-like queries
- Thread-safe with WAL mode
"""
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.shared.database import DatabaseInterface

logger = logging.getLogger(__name__)

# Default database file path
DEFAULT_DB_PATH = "data/alarm_news.db"


class SQLiteConnectionManager(DatabaseInterface):
    """
    SQLite implementation of DatabaseInterface.

    Stores documents as JSON in SQLite tables, providing MongoDB-like
    query semantics for simple operations (find, insert, update, delete).

    Each "collection" is a SQLite table with columns:
    - id: auto-increment primary key
    - data: JSON text containing the document
    - Plus indexed columns extracted from the document for fast queries
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the SQLite connection manager.

        Args:
            db_path: Path to the SQLite database file.
                     Defaults to 'data/alarm_news.db'.
        """
        self._db_path = db_path or os.environ.get("SQLITE_DB_PATH", DEFAULT_DB_PATH)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        """Create/open the SQLite database file and initialize tables."""
        # Ensure directory exists
        db_dir = os.path.dirname(self._db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30,
        )
        self._conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent read performance
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

        logger.info("SQLite database connected: %s", self._db_path)

    def disconnect(self) -> None:
        """Close the SQLite connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("SQLite database disconnected.")

    def health_check(self) -> bool:
        """Check if the database is accessible."""
        try:
            if self._conn is None:
                return False
            self._conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning("SQLite health check failed: %s", e)
            return False

    def get_collection(self, name: str) -> str:
        """Return the table name (for compatibility with interface)."""
        self._ensure_table(name)
        return name

    def insert_one(self, collection: str, document: Dict[str, Any]) -> Optional[str]:
        """Insert a document into the specified collection."""
        self._ensure_table(collection)
        doc_json = self._serialize_document(document)

        with self._lock:
            try:
                cursor = self._conn.execute(
                    f"INSERT INTO [{collection}] (data) VALUES (?)",
                    (doc_json,)
                )
                self._conn.commit()

                # Update index columns
                self._update_index_columns(collection, cursor.lastrowid, document)

                return str(cursor.lastrowid)
            except Exception as e:
                logger.error("SQLite insert_one failed (%s): %s", collection, e)
                self._conn.rollback()
                return None

    def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single document matching the query."""
        self._ensure_table(collection)
        results = self._query_documents(collection, query, limit=1)
        return results[0] if results else None

    def find_many(self, collection: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find all documents matching the query."""
        self._ensure_table(collection)
        return self._query_documents(collection, query)

    def update_one(self, collection: str, query: Dict[str, Any], update: Dict[str, Any]) -> bool:
        """Update a single document matching the query."""
        self._ensure_table(collection)
        doc = self.find_one(collection, query)
        if doc is None:
            return False

        # Apply $set operations
        if "$set" in update:
            for key, value in update["$set"].items():
                doc[key] = value
        else:
            # Direct update (merge)
            doc.update(update)

        # Remove internal _rowid
        rowid = doc.pop("_rowid", None)
        if rowid is None:
            return False

        doc_json = self._serialize_document(doc)

        with self._lock:
            try:
                self._conn.execute(
                    f"UPDATE [{collection}] SET data = ? WHERE rowid = ?",
                    (doc_json, rowid)
                )
                self._conn.commit()
                self._update_index_columns(collection, rowid, doc)
                return True
            except Exception as e:
                logger.error("SQLite update_one failed (%s): %s", collection, e)
                self._conn.rollback()
                return False

    def delete_one(self, collection: str, query: Dict[str, Any]) -> bool:
        """Delete a single document matching the query."""
        self._ensure_table(collection)
        doc = self.find_one(collection, query)
        if doc is None:
            return False

        rowid = doc.get("_rowid")
        if rowid is None:
            return False

        with self._lock:
            try:
                self._conn.execute(
                    f"DELETE FROM [{collection}] WHERE rowid = ?",
                    (rowid,)
                )
                self._conn.commit()
                return True
            except Exception as e:
                logger.error("SQLite delete_one failed (%s): %s", collection, e)
                self._conn.rollback()
                return False

    # ─── Internal Methods ────────────────────────────────────────────────

    def _ensure_table(self, name: str) -> None:
        """Create table if it doesn't exist."""
        with self._lock:
            self._conn.execute(f"""
                CREATE TABLE IF NOT EXISTS [{name}] (
                    data TEXT NOT NULL,
                    user_id TEXT,
                    subscription_expiry TEXT,
                    matched_keyword TEXT,
                    crawl_timestamp TEXT
                )
            """)
            # Create indexes
            self._conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{name}_user_id 
                ON [{name}] (user_id)
            """)
            self._conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{name}_subscription_expiry 
                ON [{name}] (subscription_expiry)
            """)
            self._conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{name}_matched_keyword 
                ON [{name}] (matched_keyword)
            """)
            self._conn.commit()

    def _update_index_columns(self, collection: str, rowid: int, document: Dict[str, Any]) -> None:
        """Update indexed columns for fast queries."""
        user_id = document.get("user_id")
        subscription_expiry = document.get("subscription_expiry")
        matched_keyword = document.get("matched_keyword")
        crawl_timestamp = document.get("crawl_timestamp")

        # Serialize datetime objects
        if isinstance(subscription_expiry, datetime):
            subscription_expiry = subscription_expiry.isoformat()
        if isinstance(crawl_timestamp, datetime):
            crawl_timestamp = crawl_timestamp.isoformat()

        self._conn.execute(
            f"""UPDATE [{collection}] SET 
                user_id = ?, subscription_expiry = ?, 
                matched_keyword = ?, crawl_timestamp = ?
                WHERE rowid = ?""",
            (user_id, subscription_expiry, matched_keyword, crawl_timestamp, rowid)
        )
        self._conn.commit()

    def _query_documents(self, collection: str, query: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Query documents with MongoDB-like filter support."""
        # Try to use indexed columns for common queries
        where_clauses = []
        params = []

        for key, value in query.items():
            if key in ("user_id", "matched_keyword") and isinstance(value, str):
                where_clauses.append(f"{key} = ?")
                params.append(value)
            elif key == "user_id" and isinstance(value, dict):
                # Skip complex queries on user_id, filter in Python
                pass
            elif key == "subscription_expiry" and isinstance(value, dict):
                # Handle $gt, $lt operators
                if "$gt" in value:
                    gt_val = value["$gt"]
                    if isinstance(gt_val, datetime):
                        gt_val = gt_val.isoformat()
                    where_clauses.append("subscription_expiry > ?")
                    params.append(str(gt_val))
                if "$lt" in value:
                    lt_val = value["$lt"]
                    if isinstance(lt_val, datetime):
                        lt_val = lt_val.isoformat()
                    where_clauses.append("subscription_expiry < ?")
                    params.append(str(lt_val))
            elif key == "matched_keyword" and isinstance(value, dict) and "$in" in value:
                placeholders = ",".join("?" * len(value["$in"]))
                where_clauses.append(f"matched_keyword IN ({placeholders})")
                params.extend(value["$in"])

        sql = f"SELECT rowid, data FROM [{collection}]"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        if limit:
            sql += f" LIMIT {limit}"

        try:
            cursor = self._conn.execute(sql, params)
            rows = cursor.fetchall()
        except Exception as e:
            logger.error("SQLite query failed (%s): %s", collection, e)
            return []

        results = []
        for row in rows:
            doc = self._deserialize_document(row["data"])
            doc["_rowid"] = row["rowid"]

            # Apply remaining filters in Python (for complex queries)
            if self._matches_query(doc, query):
                results.append(doc)

        return results

    def _matches_query(self, doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        """Check if a document matches a query filter (Python-side filtering)."""
        for key, value in query.items():
            doc_value = doc.get(key)

            if isinstance(value, dict):
                # Operator queries
                if "$gt" in value:
                    compare_val = value["$gt"]
                    if not self._compare_gt(doc_value, compare_val):
                        return False
                if "$lt" in value:
                    compare_val = value["$lt"]
                    if not self._compare_lt(doc_value, compare_val):
                        return False
                if "$gte" in value:
                    compare_val = value["$gte"]
                    if not self._compare_gte(doc_value, compare_val):
                        return False
                if "$lte" in value:
                    compare_val = value["$lte"]
                    if not self._compare_lte(doc_value, compare_val):
                        return False
                if "$in" in value:
                    if doc_value not in value["$in"]:
                        return False
            else:
                # Direct equality
                if doc_value != value:
                    return False

        return True

    def _compare_gt(self, doc_value: Any, compare_value: Any) -> bool:
        """Compare doc_value > compare_value with type coercion."""
        if doc_value is None:
            return False
        try:
            if isinstance(compare_value, datetime):
                if isinstance(doc_value, str):
                    doc_value = datetime.fromisoformat(doc_value)
                return doc_value > compare_value
            return doc_value > compare_value
        except (TypeError, ValueError):
            return False

    def _compare_lt(self, doc_value: Any, compare_value: Any) -> bool:
        """Compare doc_value < compare_value with type coercion."""
        if doc_value is None:
            return False
        try:
            if isinstance(compare_value, datetime):
                if isinstance(doc_value, str):
                    doc_value = datetime.fromisoformat(doc_value)
                return doc_value < compare_value
            return doc_value < compare_value
        except (TypeError, ValueError):
            return False

    def _compare_gte(self, doc_value: Any, compare_value: Any) -> bool:
        """Compare doc_value >= compare_value."""
        if doc_value is None:
            return False
        try:
            if isinstance(compare_value, datetime):
                if isinstance(doc_value, str):
                    doc_value = datetime.fromisoformat(doc_value)
                return doc_value >= compare_value
            return doc_value >= compare_value
        except (TypeError, ValueError):
            return False

    def _compare_lte(self, doc_value: Any, compare_value: Any) -> bool:
        """Compare doc_value <= compare_value."""
        if doc_value is None:
            return False
        try:
            if isinstance(compare_value, datetime):
                if isinstance(doc_value, str):
                    doc_value = datetime.fromisoformat(doc_value)
                return doc_value <= compare_value
            return doc_value <= compare_value
        except (TypeError, ValueError):
            return False

    def _serialize_document(self, document: Dict[str, Any]) -> str:
        """Serialize a document to JSON, handling datetime objects."""
        def default_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        # Remove internal fields
        clean_doc = {k: v for k, v in document.items() if not k.startswith("_")}
        return json.dumps(clean_doc, default=default_serializer, ensure_ascii=False)

    def _deserialize_document(self, json_str: str) -> Dict[str, Any]:
        """Deserialize a JSON string back to a document dict."""
        return json.loads(json_str)


# Module-level factory
def create_sqlite_database(db_path: Optional[str] = None) -> SQLiteConnectionManager:
    """
    Create a new SQLite connection manager instance.

    Args:
        db_path: Optional path to the database file.

    Returns:
        Connected SQLiteConnectionManager instance.
    """
    manager = SQLiteConnectionManager(db_path=db_path)
    manager.connect()
    return manager
