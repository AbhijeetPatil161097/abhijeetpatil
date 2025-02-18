class RegionTableCreator:
    """
    This class creates output tables if not alredy present in Snowflake Database.
    """

    def __init__(self, session) -> None:
        self.session = session

    def create_region_output_table_sql(self, region_output_table: str) -> str:
        """
        Creates Region Roll up output table if already does not exist in required schema.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {region_output_table} (
                PLATFORM VARCHAR(7),
                MONTH DATE,
                CHANNEL_ID VARCHAR(16777216),
                CHANNEL_NAME VARCHAR(16777216),
                VIDEO_FORM VARCHAR(16777216),
                UPLOADER_TYPE VARCHAR(16777216),
                SUB_CONTINENT VARCHAR(16777216),
                CONTINENT VARCHAR(16777216),
                AMER NUMBER(38,0),
                LATAM NUMBER(38,0),
                EMEA NUMBER(38,0),
                AMEA NUMBER(38,0),
                US_NON_US VARCHAR(6),
                VIEWS NUMBER(38,0),
                WATCH_TIME_MINUTES NUMBER(38,8),
                ESTIMATED_PARTNER_REVENUE NUMBER(38,8)
            );
        """


class YouTubeRegionQueries:
    """
    This class contains all function unique to creating region rollup output.
    """

    def __init__(self, session) -> None:
        self.session = session

    def processed_YT_Views_and_dims(
        self, input_view_data: str, start_date: str, end_date: str
    ) -> str:
        """Gather YouTube metadata from the given date range."""
        return f"""
        SELECT
            'YouTube' AS PLATFORM,
            a.REPORTING_DATE AS DATE,
            a.CHANNEL_ID,
            a.VIDEO_ID,
            a.UPLOADER_TYPE,
            a.COUNTRY_CODE,
            SUM(a.VIEWS) AS VIEWS,
            SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES
        FROM ({input_view_data}) a
        WHERE a.REPORTING_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 
            a.REPORTING_DATE, a.CHANNEL_ID, a.VIDEO_ID, a.UPLOADER_TYPE, a.COUNTRY_CODE
        """

    def processed_YT_revenue_data(
        self, input_revenue_data: str, start_date: str, end_date: str
    ) -> str:
        """Gather YouTube revenue metadata from the given date range."""
        return f"""
        SELECT
            VIDEO_ID,
            UPLOADER_TYPE,
            DATE,
            COUNTRY_CODE,
            SUM(ESTIMATED_PARTNER_REVENUE) AS ESTIMATED_PARTNER_REVENUE,
        FROM ({input_revenue_data})
        WHERE DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY VIDEO_ID, UPLOADER_TYPE, DATE, COUNTRY_CODE
        """

    def join_views_and_revenue_data(
        self, metadata_query: str, revenue_query: str
    ) -> str:
        """Join YouTube Metadata and YouTube Revenue Data."""
        return f"""
        SELECT
            a.*,
            t.ESTIMATED_PARTNER_REVENUE,
        FROM
            ({metadata_query}) a
        FULL OUTER JOIN
            ({revenue_query}) t
        ON
            a.VIDEO_ID = t.VIDEO_ID AND
            a.UPLOADER_TYPE = t.UPLOADER_TYPE AND
            a.DATE = t.DATE AND
            a.COUNTRY_CODE = t.COUNTRY_CODE
        """

    def aggregate_YT_data(self, sql_query: str, country_info_table: str) -> str:
        """Aggregate data to month level and Video_ID level."""
        return f"""
            SELECT 
                a.PLATFORM,
                LAST_DAY(TO_DATE(TO_VARCHAR(a.DATE), 'YYYYMMDD')) AS MONTH,
                a.CHANNEL_ID,
                a.CHANNEL_NAME,
                a.VIDEO_FORM,
                a.UPLOADER_TYPE,
                cg.SUB_CONTINENT,
                cg.CONTINENT,
                CASE
                    WHEN cg.CONTINENT IN ('North America', 'South America') THEN 1
                    ELSE 0
                END AS AMER,
                CASE
                    WHEN cg.SUB_CONTINENT IN ('LATIN NORTH', 'LATIN SOUTH', 'CARIBBEAN') THEN 1
                    ELSE 0
                END AS LATAM,
                CASE
                    WHEN cg.CONTINENT IN ('Africa', 'Europe') THEN 1
                    ELSE 0
                END AS EMEA,
                CASE
                    WHEN cg.CONTINENT IN ('Asia', 'Africa') THEN 1
                    ELSE 0
                END AS AMEA,
                CASE 
                    WHEN a.COUNTRY_CODE = 'US' THEN 'US' 
                    ELSE 'Non-US' 
                END AS US_NON_US,
                SUM(a.VIEWS) AS VIEWS,
                SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
                SUM(a.ESTIMATED_PARTNER_REVENUE) AS ESTIMATED_PARTNER_REVENUE
            FROM 
                ({sql_query}) a
            JOIN 
                ({country_info_table}) cg 
            ON a.COUNTRY_CODE = cg.COUNTRY_CODE
            GROUP BY 
                a.PLATFORM, 
                MONTH, 
                a.CHANNEL_ID,
                a.CHANNEL_NAME,
                a.VIDEO_FORM,
                a.UPLOADER_TYPE,
                cg.CONTINENT,
                cg.SUB_CONTINENT,
                US_NON_US
        """
