# Zaparoo Titles Database

Provides baseline metadata for matching existing ROM and Game media to known archival databases.

Essentially a fork of Libretro's Database, using NDJSON for git tracked updates.

NDJSON files may be modified directly, or mass updates may be scripted to update data from alternate data sets.

A utility command is provided to rebuild the sqlite database from NDJSON as the source of truth.

# Sources
Libretro's RDB format is already a strong aggregate of NoIntro, Redump, and TOSEC sets. \
RDB fork date: 2025-06-06