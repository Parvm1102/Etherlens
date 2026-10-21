"""Read/write helpers for local, gs://, and hdfs:// paths.

Cloud paths (gs:// and hdfs://) go through Hadoop's FileSystem API when a
Spark session is available. On Dataproc this uses the built-in GCS connector,
so the jobs need no extra Python libraries. Without Spark, gs:// falls back
to the google-cloud-storage client (used by plain-Python tools).
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path


def _is_gcs(path: str) -> bool:
    return path.startswith("gs://")


def _is_hdfs(path: str) -> bool:
    return path.startswith("hdfs://")


def is_cloud(path: str) -> bool:
    """True for gs:// and hdfs:// paths."""
    return _is_gcs(path) or _is_hdfs(path)


def _hadoop_fs(spark, path: str):
    """Return (FileSystem, Path) for a cloud path via the Spark JVM."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), conf)
    return fs, jvm.org.apache.hadoop.fs.Path(path)


def _gcs_blob(path: str):
    from google.cloud import storage as gcs
    bucket_name, _, blob_name = path[len("gs://"):].partition("/")
    return gcs.Client().bucket(bucket_name).blob(blob_name)


def makedirs(path: str) -> None:
    """Create a local directory; no-op for cloud paths (they have no real dirs)."""
    if not is_cloud(path):
        os.makedirs(path, exist_ok=True)


def exists(spark_or_none, path: str) -> bool:
    """Check if a path exists (local, gs://, or hdfs://)."""
    if is_cloud(path):
        if spark_or_none is None:
            raise ValueError(f"Need a Spark session to check cloud path: {path}")
        fs, p = _hadoop_fs(spark_or_none, path)
        return bool(fs.exists(p))
    return Path(path).exists()


def delete(spark_or_none, path: str) -> None:
    """Recursively delete a file or directory if it exists."""
    if is_cloud(path):
        fs, p = _hadoop_fs(spark_or_none, path)
        if fs.exists(p):
            fs.delete(p, True)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def size_bytes(spark_or_none, path: str) -> int:
    """Total size in bytes of a file or directory tree."""
    if is_cloud(path):
        fs, p = _hadoop_fs(spark_or_none, path)
        return int(fs.getContentSummary(p).getLength())
    p = Path(path)
    if p.is_file():
        return p.stat().st_size
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def write_text(spark_or_none, path: str, text: str) -> None:
    """Write text to local, gs://, or hdfs:// path (whole-file overwrite)."""
    if is_cloud(path) and spark_or_none is not None:
        fs, p = _hadoop_fs(spark_or_none, path)
        out = fs.create(p, True)
        out.write(bytearray(text.encode("utf-8")))
        out.close()
    elif _is_gcs(path):
        _gcs_blob(path).upload_from_string(text, content_type="text/plain")
    elif _is_hdfs(path):
        raise ValueError(f"Need a Spark session to write HDFS path: {path}")
    else:
        # Local: write a temp file then rename, so readers never see half a file
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.parent / f".{p.name}.tmp"
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, p)


def read_text(spark_or_none, path: str) -> str:
    """Read text from local, gs://, or hdfs:// path."""
    if is_cloud(path) and spark_or_none is not None:
        jvm = spark_or_none._jvm
        fs, p = _hadoop_fs(spark_or_none, path)
        stream = fs.open(p)
        reader = jvm.java.io.BufferedReader(
            jvm.java.io.InputStreamReader(stream, "UTF-8"))
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        return "\n".join(lines)
    elif _is_gcs(path):
        return _gcs_blob(path).download_as_text()
    elif _is_hdfs(path):
        raise ValueError(f"Need a Spark session to read HDFS path: {path}")
    else:
        return Path(path).read_text(encoding="utf-8")


def write_json(spark_or_none, path: str, obj) -> None:
    """Write JSON to a path."""
    write_text(spark_or_none, path, json.dumps(obj, indent=2))


def read_json(spark_or_none, path: str):
    """Read JSON from a path."""
    return json.loads(read_text(spark_or_none, path))
