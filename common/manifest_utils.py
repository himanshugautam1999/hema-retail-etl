
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from common.logging_utils import get_logger

# Table property that makes Delta regenerate the manifest on every write, so the
# external table never points at a stale file list.
AUTO_MANIFEST_PROP = (
    "delta.compatibility.symlinkFormatManifest.enabled = true"
)


def enable_auto_manifest(spark: SparkSession, table_path: str) -> None:
    """
    Turn on automatic manifest regeneration for a Delta table
    Safe to call on every run , setting an already-set property is a no-op
    """
    log = get_logger("manifest", {"path": table_path})
    spark.sql(
        f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES ({AUTO_MANIFEST_PROP})"
    )
    log.info("Auto symlink-manifest regeneration enabled.")


def generate_manifest(spark: SparkSession, table_path: str) -> None:
    """
    Generate the symlink manifest once
    auto property: guarantees a manifest exists even on the first write, before
    the property has taken effect for subsequent versions.
    """
    log = get_logger("manifest", {"path": table_path})
    DeltaTable.forPath(spark, table_path).generate("symlink_format_manifest")
    log.info("Symlink manifest generated.")
