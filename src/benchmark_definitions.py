from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BenchmarkQuery:
    query_id: str
    name: str
    sql: str
    experiment_type: str
    description: str


QUERIES = [
    BenchmarkQuery(
        query_id="Q01",
        name="trajectory_projection_scan",
        experiment_type="format",
        description="Projection scan on trajectory columns inside a fixed time window.",
        sql="""
        SELECT
            pickup_ts,
            origin_zone_id,
            destination_zone_id,
            trip_distance,
            total_amt
        FROM trips_clean
        WHERE pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
        """,
    ),
    BenchmarkQuery(
        query_id="Q02",
        name="time_filter_partition_pruning",
        experiment_type="partition",
        description="Time-window filter used to verify partition pruning on trip_year and trip_month.",
        sql="""
        SELECT COUNT(*) AS trip_count
        FROM trips_clean
        WHERE trip_year = {window_year}
          AND trip_month = {window_month}
          AND pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
        """,
    ),
    BenchmarkQuery(
        query_id="Q03",
        name="vendor_origin_zone_aggregation",
        experiment_type="format",
        description="Aggregate trips by vendor and origin zone to compare scan and grouping cost.",
        sql="""
        SELECT
            vendor_name_norm,
            origin_zone_id,
            COUNT(*) AS trip_count,
            AVG(total_amt) AS avg_total_amt,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
          AND origin_zone_id IS NOT NULL
        GROUP BY vendor_name_norm, origin_zone_id
        ORDER BY trip_count DESC
        LIMIT 50
        """,
    ),
    BenchmarkQuery(
        query_id="Q04",
        name="origin_zone_skew_aggregation",
        experiment_type="join_skew",
        description="Group by origin zone to surface naturally skewed spatial keys.",
        sql="""
        SELECT
            origin_zone_id,
            COUNT(*) AS trip_count,
            SUM(total_amt) AS total_revenue,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
          AND origin_zone_id IS NOT NULL
        GROUP BY origin_zone_id
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
    BenchmarkQuery(
        query_id="Q05",
        name="daily_zone_flow_summary",
        experiment_type="partition",
        description="Daily zone-based flow summary over the benchmark time window.",
        sql="""
        SELECT
            trip_date,
            origin_zone_id,
            COUNT(*) AS trip_count,
            SUM(total_amt) AS total_revenue,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE trip_year = {window_year}
          AND trip_month = {window_month}
          AND pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
          AND origin_zone_id IS NOT NULL
        GROUP BY trip_date, origin_zone_id
        ORDER BY trip_date, trip_count DESC
        LIMIT 500
        """,
    ),
    BenchmarkQuery(
        query_id="Q06",
        name="route_ranking_window",
        experiment_type="format",
        description="Window-based route ranking for trajectory analytics.",
        sql="""
        WITH route_daily AS (
            SELECT
                trip_date,
                route_key,
                COUNT(*) AS trip_count,
                SUM(total_amt) AS total_revenue
            FROM trips_clean
            WHERE trip_year = {window_year}
              AND trip_month = {window_month}
              AND pickup_ts >= TIMESTAMP '{window_start}'
              AND pickup_ts < TIMESTAMP '{window_end}'
              AND route_key IS NOT NULL
            GROUP BY trip_date, route_key
        ),
        ranked AS (
            SELECT
                trip_date,
                route_key,
                trip_count,
                total_revenue,
                DENSE_RANK() OVER (
                    PARTITION BY trip_date
                    ORDER BY trip_count DESC
                ) AS route_rank
            FROM route_daily
        )
        SELECT *
        FROM ranked
        WHERE route_rank <= 10
        ORDER BY trip_date, route_rank, route_key
        """,
    ),
    BenchmarkQuery(
        query_id="Q07",
        name="origin_destination_volume",
        experiment_type="partition",
        description="Monthly origin-destination volume and revenue summary.",
        sql="""
        SELECT
            trip_year,
            trip_month,
            origin_zone_id,
            destination_zone_id,
            COUNT(*) AS trip_count,
            AVG(total_amt) AS avg_total_amt,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE trip_year = {window_year}
          AND trip_month = {window_month}
          AND pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
          AND origin_zone_id IS NOT NULL
          AND destination_zone_id IS NOT NULL
        GROUP BY trip_year, trip_month, origin_zone_id, destination_zone_id
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
    BenchmarkQuery(
        query_id="Q08",
        name="zone_join_baseline",
        experiment_type="join_skew",
        description="Join trajectory records with the taxi zone dimension on origin zone.",
        sql="""
        SELECT /*+ MERGE(t, z) */
            z.borough,
            z.zone,
            COUNT(*) AS trip_count,
            SUM(t.total_amt) AS total_revenue
        FROM trips_clean t
        JOIN taxi_zone_dim z
          ON t.origin_zone_id = z.location_id
        WHERE t.pickup_ts >= TIMESTAMP '{window_start}'
          AND t.pickup_ts < TIMESTAMP '{window_end}'
        GROUP BY z.borough, z.zone
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
    BenchmarkQuery(
        query_id="Q09",
        name="zone_broadcast_join",
        experiment_type="join_skew",
        description="Broadcast join trajectory records with the taxi zone dimension.",
        sql="""
        SELECT /*+ BROADCAST(z) */
            z.borough,
            z.zone,
            COUNT(*) AS trip_count,
            SUM(t.total_amt) AS total_revenue
        FROM trips_clean t
        JOIN taxi_zone_dim z
          ON t.origin_zone_id = z.location_id
        WHERE t.pickup_ts >= TIMESTAMP '{window_start}'
          AND t.pickup_ts < TIMESTAMP '{window_end}'
        GROUP BY z.borough, z.zone
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
    BenchmarkQuery(
        query_id="Q10",
        name="salted_zone_join",
        experiment_type="join_skew",
        description="Salted join on origin zone to test skew mitigation on a spatial key.",
        sql="""
        WITH trips_salted AS (
            SELECT
                *,
                PMOD(HASH(CONCAT(CAST(origin_zone_id AS STRING), CAST(trip_date AS STRING))), 8) AS salt_key
            FROM trips_clean
            WHERE pickup_ts >= TIMESTAMP '{window_start}'
              AND pickup_ts < TIMESTAMP '{window_end}'
              AND origin_zone_id IS NOT NULL
        ),
        taxi_zone_dim_salted AS (
            SELECT
                location_id,
                borough,
                zone,
                service_zone,
                salt_key
            FROM taxi_zone_dim
            CROSS JOIN RANGE(0, 8) AS salts(salt_key)
        )
        SELECT
            z.borough,
            z.zone,
            COUNT(*) AS trip_count,
            SUM(t.total_amt) AS total_revenue
        FROM trips_salted t
        JOIN taxi_zone_dim_salted z
          ON t.origin_zone_id = z.location_id
         AND t.salt_key = z.salt_key
        GROUP BY z.borough, z.zone
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
    BenchmarkQuery(
        query_id="Q11",
        name="bucketed_origin_zone_lookup",
        experiment_type="bucketing",
        description="Bucket-targeted route lookup for a selected origin zone to validate hash-bucketed data skipping.",
        sql="""
        SELECT
            origin_zone_id,
            destination_zone_id,
            COUNT(*) AS trip_count,
            AVG(total_amt) AS avg_total_amt,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE pickup_ts >= TIMESTAMP '{window_start}'
          AND pickup_ts < TIMESTAMP '{window_end}'
          AND origin_zone_bucket = {bucket_value}
          AND origin_zone_id = {bucket_zone_id}
          AND destination_zone_id IS NOT NULL
        GROUP BY origin_zone_id, destination_zone_id
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
    BenchmarkQuery(
        query_id="Q12",
        name="bucketed_origin_zone_join",
        experiment_type="bucketing",
        description="Bucket-targeted zone join on the selected origin zone to compare raw and hash-bucketed layouts.",
        sql="""
        SELECT
            z.borough,
            z.zone,
            COUNT(*) AS trip_count,
            SUM(t.total_amt) AS total_revenue,
            AVG(t.trip_distance) AS avg_trip_distance
        FROM trips_clean t
        JOIN taxi_zone_dim z
          ON t.origin_zone_id = z.location_id
        WHERE t.pickup_ts >= TIMESTAMP '{window_start}'
          AND t.pickup_ts < TIMESTAMP '{window_end}'
          AND t.origin_zone_bucket = {bucket_value}
          AND t.origin_zone_id = {bucket_zone_id}
        GROUP BY z.borough, z.zone
        ORDER BY trip_count DESC
        LIMIT 20
        """,
    ),
]


QUERY_MAP = {query.query_id: query for query in QUERIES}
ALL_QUERY_IDS = [query.query_id for query in QUERIES]
QUERY_GROUPS = {
    "format": ["Q01", "Q03", "Q06", "Q07"],
    "partition": ["Q02", "Q05", "Q06", "Q07"],
    "join_skew": ["Q04", "Q08", "Q09", "Q10"],
    "bucket_layout": ["Q11", "Q12"],
}
