"""GDELT 2.0 Event Database schema.

The export CSVs are tab-delimited, have **no header row**, and carry 61 fixed
columns. Ordering here is authoritative and matches the GDELT 2.0 codebook
(http://data.gdeltproject.org/documentation/GDELT-Event_Data_Format_Codebook.pdf).
Downstream Spark parsing zips this list onto the raw columns.
"""

from __future__ import annotations

# (column_name, semantic_type) — semantic_type drives Spark casting in silver.
EVENT_COLUMNS: list[tuple[str, str]] = [
    ("global_event_id", "long"),
    ("sql_date", "date_yyyymmdd"),
    ("month_year", "int"),
    ("year", "int"),
    ("fraction_date", "double"),
    # Actor 1
    ("actor1_code", "string"),
    ("actor1_name", "string"),
    ("actor1_country_code", "string"),
    ("actor1_known_group_code", "string"),
    ("actor1_ethnic_code", "string"),
    ("actor1_religion1_code", "string"),
    ("actor1_religion2_code", "string"),
    ("actor1_type1_code", "string"),
    ("actor1_type2_code", "string"),
    ("actor1_type3_code", "string"),
    # Actor 2
    ("actor2_code", "string"),
    ("actor2_name", "string"),
    ("actor2_country_code", "string"),
    ("actor2_known_group_code", "string"),
    ("actor2_ethnic_code", "string"),
    ("actor2_religion1_code", "string"),
    ("actor2_religion2_code", "string"),
    ("actor2_type1_code", "string"),
    ("actor2_type2_code", "string"),
    ("actor2_type3_code", "string"),
    # Event action
    ("is_root_event", "int"),
    ("event_code", "string"),
    ("event_base_code", "string"),
    ("event_root_code", "string"),
    ("quad_class", "int"),
    ("goldstein_scale", "double"),
    ("num_mentions", "int"),
    ("num_sources", "int"),
    ("num_articles", "int"),
    ("avg_tone", "double"),
    # Actor1 geography
    ("actor1_geo_type", "int"),
    ("actor1_geo_fullname", "string"),
    ("actor1_geo_country_code", "string"),
    ("actor1_geo_adm1_code", "string"),
    ("actor1_geo_adm2_code", "string"),
    ("actor1_geo_lat", "double"),
    ("actor1_geo_long", "double"),
    ("actor1_geo_feature_id", "string"),
    # Actor2 geography
    ("actor2_geo_type", "int"),
    ("actor2_geo_fullname", "string"),
    ("actor2_geo_country_code", "string"),
    ("actor2_geo_adm1_code", "string"),
    ("actor2_geo_adm2_code", "string"),
    ("actor2_geo_lat", "double"),
    ("actor2_geo_long", "double"),
    ("actor2_geo_feature_id", "string"),
    # Action geography
    ("action_geo_type", "int"),
    ("action_geo_fullname", "string"),
    ("action_geo_country_code", "string"),
    ("action_geo_adm1_code", "string"),
    ("action_geo_adm2_code", "string"),
    ("action_geo_lat", "double"),
    ("action_geo_long", "double"),
    ("action_geo_feature_id", "string"),
    # Data management
    ("date_added", "timestamp_yyyymmddhhmmss"),
    ("source_url", "string"),
]

EVENT_COLUMN_NAMES: list[str] = [name for name, _ in EVENT_COLUMNS]

# The schema contract: a conformant GDELT export row has exactly this many
# tab-delimited fields. A different count signals structural schema drift
# (a column added/removed) or a malformed row.
EXPECTED_COLUMN_COUNT: int = len(EVENT_COLUMNS)

assert EXPECTED_COLUMN_COUNT == 61, f"GDELT event schema must be 61 cols, got {EXPECTED_COLUMN_COUNT}"
