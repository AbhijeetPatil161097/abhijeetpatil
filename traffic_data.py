class TrafficTableCreator:
    """
    This class creates output tables if not alredy present in Snowflake Database.
    """

    def __init__(self, session) -> None:
        self.session = session

    def create_traffic_output_table_sql(self, traffic_output_table: str) -> str:
        """
        Creates Traffic Source output table if already does not exist in required schema.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {traffic_output_table} (
                PLATFORM VARCHAR(7),
                MONTH DATE,
                SERIES_ID NUMBER(38,0),
                SERIES_NAME VARCHAR(16777216),
                PARENT_SERIES_ID NUMBER(38,0),
                PARENT_SERIES_NAME VARCHAR(16777216),
                CHANNEL_ID VARCHAR(16777216),
                CHANNEL_NAME VARCHAR(16777216),
                BRAND_ROLL_UP VARCHAR(16777216),
                VIDEO_FORM VARCHAR(16777216),
                TRAFFIC_SOURCE_TYPE_CATEGORY VARCHAR(100),
                UPLOADER_TYPE VARCHAR(100),
                US_NON_US VARCHAR(6),
                VIEWS NUMBER(38,0),
                WATCH_TIME_MINUTES NUMBER(38,8)
            );
        """


class TrafficSourceQueries:
    """
    This class contains all function unique to creating Traffic source output.
    """

    def __init__(self, session) -> None:
        self.session = session

    def processed_YT_views_data(
        self, input_table: str, traffic_dim_table: str, start_date: str, end_date: str
    ) -> str:
        """
        Function:
            Gather YouTube metadata from given date range.
        """
        return f"""
            SELECT
                'YouTube' AS PLATFORM,
                a.DATE,
                a.VIDEO_ID,
                a.UPLOADER_TYPE,
                a.CHANNEL_ID,
                CASE 
                    WHEN a.COUNTRY_CODE = 'US' THEN 'US' 
                    ELSE 'Non-US' 
                END AS US_NON_US,
                b.TRAFFIC_SOURCE_TYPE_NAME,
                SUM(a.VIEWS) AS VIEWS,
                SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES
            FROM 
                {input_table} a
            LEFT JOIN 
                {traffic_dim_table} b 
            ON a.TRAFFIC_SOURCE_TYPE = b.TRAFFIC_SOURCE_TYPE_ID
            WHERE 
                a.DATE BETWEEN '{start_date}' AND '{end_date}' AND a.UPLOADER_TYPE = 'self'
            GROUP BY ALL
        """

    def aggregate_data(self, traffic_source_query: str) -> str:
        return f"""
            SELECT 
                PLATFORM,
                LAST_DAY(TO_DATE(TO_VARCHAR(DATE), 'YYYYMMDD')) AS MONTH,
                SERIES_ID,
                SERIES_NAME,
                PARENT_SERIES_ID,
                PARENT_SERIES_NAME,
                CHANNEL_ID,
                CHANNEL_NAME,
                BRAND_ROLL_UP,
                VIDEO_FORM,
                CASE
                    WHEN TRAFFIC_SOURCE_TYPE_NAME IN (
                        'Browse features', 
                        'YouTube search', 
                        'Suggested videos', 
                        'External', 
                        'YouTube advertising'
                        )
                    THEN TRAFFIC_SOURCE_TYPE_NAME
                    WHEN TRAFFIC_SOURCE_TYPE_NAME IN ('Playlists', 'Playlist pages')
                    THEN 'Playlists'
                    ELSE 'Other'
                END AS TRAFFIC_SOURCE_TYPE_CATEGORY,
                UPLOADER_TYPE,
                US_NON_US,
                SUM(VIEWS) AS VIEWS,
                SUM(WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES
            FROM ({traffic_source_query})
            GROUP BY ALL
        """
