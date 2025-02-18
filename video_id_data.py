class Video_Id_TableCreator:
    """
    This class creates output tables if not alredy present in Snowflake Database.
    """

    def __init__(self, session) -> None:
        self.session = session

    def create_monthly_video_id_output_table_sql(
        self, monthly_video_id_table: str
    ) -> str:
        """
        Creates Core output table if already does not exist in required schema.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {monthly_video_id_table} (
                PLATFORM VARCHAR(7),
                MONTH DATE,
                CHANNEL_ID VARCHAR(16777216),
                VIDEO_ID VARCHAR(16777216),
                UPLOADER_TYPE VARCHAR(16777216),
                GEO VARCHAR(6),
                VIEWS NUMBER(38,0),
                COMMENTS NUMBER(38,0),
                SHARES NUMBER(38,0),
                LIKES NUMBER(38,0),
                DISLIKES NUMBER(38,0),
                WATCH_TIME_MINUTES NUMBER(38,4),
                SUBSCRIBERS_GAINED NUMBER(38,0),
                SUBSCRIBERS_LOST NUMBER(38,0),
                AD_IMPRESSIONS NUMBER(38,0),
                ESTIMATED_PARTNER_REVENUE NUMBER(38,12),
                ESTIMATED_YOUTUBE_AD_REVENUE NUMBER(38,12),
                ANNOTATION_IMPRESSIONS NUMBER(38,0),
                ANNOTATION_CLICKABLE_IMPRESSIONS NUMBER(38,0),
                ANNOTATION_CLOSABLE_IMPRESSIONS NUMBER(38,0),
                CARD_TEASER_IMPRESSIONS NUMBER(38,0),
                CARD_IMPRESSIONS NUMBER(38,0),
                ESTIMATED_CPM NUMBER(38,12),
                CALCULATED_CPM NUMBER(38,3),
                ESTIMATED_MONETIZED_PLAYBACKS NUMBER(38,0),
                PUBLISHED_DATE VARCHAR(16777216),
                VIDEO_TITLE VARCHAR(16777216),
                DURATION NUMBER(38,0),
                CHANNEL_NAME VARCHAR(16777216),
                EXTRACTED_PPL_ID NUMBER(38,0),
                SERIES_ID NUMBER(38,5),
                SERIES_NAME VARCHAR(16777216),
                PARENT_SERIES_ID VARCHAR(16777216),
                PARENT_SERIES_NAME VARCHAR(16777216),
                BRAND_ROLL_UP VARCHAR(14),
                VIDEO_FORM VARCHAR(16)
            );
        """

    def create_daily_video_id_output_table_sql(self, daily_video_id_table: str) -> str:
        """
        Creates Core output table if already does not exist in required schema.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {daily_video_id_table} (
                PLATFORM VARCHAR(7),
                DATE DATE,
                CHANNEL_ID VARCHAR(16777216),
                VIDEO_ID VARCHAR(16777216),
                UPLOADER_TYPE VARCHAR(16777216),
                GEO VARCHAR(6),
                VIEWS NUMBER(38,0),
                COMMENTS NUMBER(38,0),
                SHARES NUMBER(38,0),
                LIKES NUMBER(38,0),
                DISLIKES NUMBER(38,0),
                WATCH_TIME_MINUTES NUMBER(38,4),
                SUBSCRIBERS_GAINED NUMBER(38,0),
                SUBSCRIBERS_LOST NUMBER(38,0),
                AD_IMPRESSIONS NUMBER(38,0),
                ESTIMATED_PARTNER_REVENUE NUMBER(38,12),
                ESTIMATED_YOUTUBE_AD_REVENUE NUMBER(38,12),
                ANNOTATION_IMPRESSIONS NUMBER(38,0),
                ANNOTATION_CLICKABLE_IMPRESSIONS NUMBER(38,0),
                ANNOTATION_CLOSABLE_IMPRESSIONS NUMBER(38,0),
                CARD_TEASER_IMPRESSIONS NUMBER(38,0),
                CARD_IMPRESSIONS NUMBER(38,0),
                ESTIMATED_CPM NUMBER(38,12),
                CALCULATED_CPM NUMBER(38,3),
                ESTIMATED_MONETIZED_PLAYBACKS NUMBER(38,0),
                PUBLISHED_DATE VARCHAR(16777216),
                VIDEO_TITLE VARCHAR(16777216),
                DURATION NUMBER(38,0),
                CHANNEL_NAME VARCHAR(16777216),
                EXTRACTED_PPL_ID NUMBER(38,0),
                SERIES_ID NUMBER(38,5),
                SERIES_NAME VARCHAR(16777216),
                PARENT_SERIES_ID VARCHAR(16777216),
                PARENT_SERIES_NAME VARCHAR(16777216),
                BRAND_ROLL_UP VARCHAR(14),
                VIDEO_FORM VARCHAR(16)
            );
        """


class Video_Id_Queries:
    """
    This class contains all function unique to creating CORE output.
    """

    def __init__(self, session) -> None:
        self.session = session

    def processed_YT_Views_and_dims(
        self, input_view_data, start_date: str, end_date: str
    ) -> str:
        """Gather YouTube metadata from the given date range."""
        return f"""
        SELECT
            a.REPORTING_DATE AS DATE,
            a.CHANNEL_ID,
            a.VIDEO_ID,
            a.UPLOADER_TYPE,
            a.COUNTRY_CODE,
            SUM(a.VIEWS) AS VIEWS,
            SUM(a.COMMENTS) AS COMMENTS,
            SUM(a.SHARES) AS SHARES,
            SUM(a.LIKES) AS LIKES,
            SUM(a.DISLIKES) AS DISLIKES,
            SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
            SUM(a.SUBSCRIBERS_GAINED) AS SUBSCRIBERS_GAINED,
            SUM(a.SUBSCRIBERS_LOST) AS SUBSCRIBERS_LOST,
            AVG(a.ANNOTATION_CLICK_THROUGH_RATE) AS ANNOTATION_CLICK_THROUGH_RATE,
            SUM(a.ANNOTATION_IMPRESSIONS) AS ANNOTATION_IMPRESSIONS,
            SUM(a.ANNOTATION_CLICKABLE_IMPRESSIONS) AS ANNOTATION_CLICKABLE_IMPRESSIONS,
            SUM(a.ANNOTATION_CLOSABLE_IMPRESSIONS) AS ANNOTATION_CLOSABLE_IMPRESSIONS,
            SUM(a.CARD_TEASER_IMPRESSIONS) AS CARD_TEASER_IMPRESSIONS,
            SUM(a.CARD_IMPRESSIONS) AS CARD_IMPRESSIONS
        FROM ({input_view_data}) a
        WHERE a.REPORTING_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 
            ALL
        """

    def processed_YT_revenue_data(
        self, input_revenue_data, start_date: str, end_date: str
    ) -> str:
        """Gather YouTube revenue metadata from the given date range."""
        return f"""
        SELECT
            VIDEO_ID,
            CHANNEL_ID,
            UPLOADER_TYPE,
            DATE,
            COUNTRY_CODE,
            SUM(AD_IMPRESSIONS) AS AD_IMPRESSIONS,
            SUM(ESTIMATED_PARTNER_REVENUE) AS ESTIMATED_PARTNER_REVENUE,
            SUM(ESTIMATED_YOUTUBE_AD_REVENUE) AS ESTIMATED_YOUTUBE_AD_REVENUE,
            AVG(ESTIMATED_CPM) AS ESTIMATED_CPM,
            SUM(ESTIMATED_MONETIZED_PLAYBACKS) AS ESTIMATED_MONETIZED_PLAYBACKS
        FROM ({input_revenue_data})
        WHERE DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 
            ALL
        """

    def join_views_and_revenue_data(
        self, metadata_query: str, revenue_query: str
    ) -> str:
        """Join YouTube Metadata and YouTube Revenue Data."""
        return f"""
        SELECT
            'YouTube' AS PLATFORM,
            COALESCE(a.DATE, t.DATE) AS DATE,
            COALESCE(a.CHANNEL_ID, t.CHANNEL_ID) AS CHANNEL_ID,
            COALESCE(a.VIDEO_ID, t.VIDEO_ID) AS VIDEO_ID,
            COALESCE(a.UPLOADER_TYPE, t.UPLOADER_TYPE) AS UPLOADER_TYPE,
            COALESCE(a.COUNTRY_CODE, t.COUNTRY_CODE) AS COUNTRY_CODE,
            SUM(a.VIEWS) AS VIEWS,
            SUM(a.COMMENTS) AS COMMENTS,
            SUM(a.SHARES) AS SHARES,
            SUM(a.LIKES) AS LIKES,
            SUM(a.DISLIKES) AS DISLIKES,
            SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
            SUM(a.SUBSCRIBERS_GAINED) AS SUBSCRIBERS_GAINED,
            SUM(a.SUBSCRIBERS_LOST) AS SUBSCRIBERS_LOST,
            AVG(a.ANNOTATION_CLICK_THROUGH_RATE) AS ANNOTATION_CLICK_THROUGH_RATE,
            SUM(a.ANNOTATION_IMPRESSIONS) AS ANNOTATION_IMPRESSIONS,
            SUM(a.ANNOTATION_CLICKABLE_IMPRESSIONS) AS ANNOTATION_CLICKABLE_IMPRESSIONS,
            SUM(a.ANNOTATION_CLOSABLE_IMPRESSIONS) AS ANNOTATION_CLOSABLE_IMPRESSIONS,
            SUM(a.CARD_TEASER_IMPRESSIONS) AS CARD_TEASER_IMPRESSIONS,
            SUM(a.CARD_IMPRESSIONS) AS CARD_IMPRESSIONS,
            SUM(t.AD_IMPRESSIONS) AS AD_IMPRESSIONS,
            SUM(t.ESTIMATED_PARTNER_REVENUE) AS ESTIMATED_PARTNER_REVENUE,
            SUM(t.ESTIMATED_YOUTUBE_AD_REVENUE) AS ESTIMATED_YOUTUBE_AD_REVENUE,
            AVG(t.ESTIMATED_CPM) as ESTIMATED_CPM,
            SUM(t.ESTIMATED_MONETIZED_PLAYBACKS) AS ESTIMATED_MONETIZED_PLAYBACKS
        FROM
            ({metadata_query}) a
        FULL OUTER JOIN
            ({revenue_query}) t
        ON
            a.VIDEO_ID = t.VIDEO_ID AND
            a.CHANNEL_ID = t.CHANNEL_ID AND
            a.UPLOADER_TYPE = t.UPLOADER_TYPE AND
            a.DATE = t.DATE AND
            a.COUNTRY_CODE = t.COUNTRY_CODE
        group by all
        """

    def aggregate_YT_data_monthly(self, sql_query: str) -> str:
        """Aggregate data to month level and Video_ID level."""
        return f"""
            SELECT 
                a.PLATFORM,
                LAST_DAY(TO_DATE(TO_VARCHAR(a.DATE), 'YYYYMMDD')) AS MONTH,
                a.CHANNEL_ID,
                a.VIDEO_ID,
                a.UPLOADER_TYPE,
                CASE 
                    WHEN a.COUNTRY_CODE = 'US' THEN 'US' 
                    ELSE 'Non-US' 
                END AS GEO,
                SUM(a.VIEWS) AS VIEWS,
                SUM(a.COMMENTS) AS COMMENTS,
                SUM(a.SHARES) AS SHARES,
                SUM(a.LIKES) AS LIKES,
                SUM(a.DISLIKES) AS DISLIKES,
                SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
                SUM(a.SUBSCRIBERS_GAINED) AS SUBSCRIBERS_GAINED,
                SUM(a.SUBSCRIBERS_LOST) AS SUBSCRIBERS_LOST,
                SUM(a.AD_IMPRESSIONS) AS AD_IMPRESSIONS,
                SUM(a.ESTIMATED_PARTNER_REVENUE) AS ESTIMATED_PARTNER_REVENUE,
                SUM(a.ESTIMATED_YOUTUBE_AD_REVENUE) AS ESTIMATED_YOUTUBE_AD_REVENUE,
                SUM(a.ANNOTATION_IMPRESSIONS) AS ANNOTATION_IMPRESSIONS,
                SUM(a.ANNOTATION_CLICKABLE_IMPRESSIONS) AS ANNOTATION_CLICKABLE_IMPRESSIONS,
                SUM(a.ANNOTATION_CLOSABLE_IMPRESSIONS) AS ANNOTATION_CLOSABLE_IMPRESSIONS,
                SUM(a.CARD_TEASER_IMPRESSIONS) AS CARD_TEASER_IMPRESSIONS,
                SUM(a.CARD_IMPRESSIONS) AS CARD_IMPRESSIONS,
                AVG(a.ESTIMATED_CPM) AS ESTIMATED_CPM,
                ROUND(
                    COALESCE((sum(a.ESTIMATED_YOUTUBE_AD_REVENUE) / NULLIF(sum(a.AD_IMPRESSIONS), 0)) * 1000, 0),
                    3
                ) AS CALCULATED_CPM,
                SUM(a.ESTIMATED_MONETIZED_PLAYBACKS) AS ESTIMATED_MONETIZED_PLAYBACKS
            FROM 
                ({sql_query}) a
            GROUP BY 
                a.PLATFORM, MONTH, a.CHANNEL_ID, a.VIDEO_ID, a.UPLOADER_TYPE, GEO
        """

    def aggregate_YT_data_daily(self, sql_query: str) -> str:
        """Aggregate data to month level and Video_ID level."""
        return f"""
            SELECT 
                PLATFORM,
                TO_DATE(CAST(a.DATE AS STRING), 'YYYYMMDD') AS DATE,
                CHANNEL_ID,
                VIDEO_ID,
                UPLOADER_TYPE,
                CASE 
                    WHEN COUNTRY_CODE = 'US' THEN 'US' 
                    ELSE 'Non-US' 
                END AS GEO,
                SUM(VIEWS) AS VIEWS,
                SUM(COMMENTS) AS COMMENTS,
                SUM(SHARES) AS SHARES,
                SUM(LIKES) AS LIKES,
                SUM(DISLIKES) AS DISLIKES,
                SUM(WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
                SUM(SUBSCRIBERS_GAINED) AS SUBSCRIBERS_GAINED,
                SUM(SUBSCRIBERS_LOST) AS SUBSCRIBERS_LOST,
                SUM(AD_IMPRESSIONS) AS AD_IMPRESSIONS,
                SUM(ESTIMATED_PARTNER_REVENUE) AS ESTIMATED_PARTNER_REVENUE,
                SUM(ESTIMATED_YOUTUBE_AD_REVENUE) AS ESTIMATED_YOUTUBE_AD_REVENUE,
                SUM(ANNOTATION_IMPRESSIONS) AS ANNOTATION_IMPRESSIONS,
                SUM(ANNOTATION_CLICKABLE_IMPRESSIONS) AS ANNOTATION_CLICKABLE_IMPRESSIONS,
                SUM(ANNOTATION_CLOSABLE_IMPRESSIONS) AS ANNOTATION_CLOSABLE_IMPRESSIONS,
                SUM(CARD_TEASER_IMPRESSIONS) AS CARD_TEASER_IMPRESSIONS,
                SUM(CARD_IMPRESSIONS) AS CARD_IMPRESSIONS,
                AVG(ESTIMATED_CPM) AS ESTIMATED_CPM,
                ROUND(
                    COALESCE((SUM(ESTIMATED_YOUTUBE_AD_REVENUE) / NULLIF(SUM(AD_IMPRESSIONS), 0)) * 1000, 0),
                    3
                ) AS CALCULATED_CPM,
                SUM(ESTIMATED_MONETIZED_PLAYBACKS) AS ESTIMATED_MONETIZED_PLAYBACKS
            FROM 
                ({sql_query}) a
            GROUP BY 
                PLATFORM, DATE, CHANNEL_ID, VIDEO_ID, UPLOADER_TYPE, GEO
            """

    def reorder_table_columns(self, query: str, date_col: str) -> str:
        """
        Function:
            * This function reorders the columns in the output table.

        Params:
            * query: str: SQL query to reorder the columns in the output table.
        """
        return f"""
            WITH final_data AS (
                SELECT
                    PLATFORM,
                    {date_col},
                    PUBLISHED_DATE,
                    VIDEO_ID,
                    VIDEO_TITLE,
                    SERIES_ID,
                    PARENT_SERIES_ID,
                    SERIES_NAME,
                    PARENT_SERIES_NAME,
                    CHANNEL_ID,
                    CHANNEL_NAME,
                    EXTRACTED_PPL_ID,
                    GEO,
                    UPLOADER_TYPE,
                    BRAND_ROLL_UP,
                    VIDEO_FORM,
                    DURATION,
                    VIEWS,
                    LIKES,
                    DISLIKES,
                    COMMENTS,
                    SHARES,
                    WATCH_TIME_MINUTES,
                    SUBSCRIBERS_GAINED,
                    SUBSCRIBERS_LOST,
                    AD_IMPRESSIONS,
                    ESTIMATED_PARTNER_REVENUE,
                    ESTIMATED_YOUTUBE_AD_REVENUE,
                    ESTIMATED_CPM,
                    ESTIMATED_MONETIZED_PLAYBACKS,
                    ANNOTATION_CLICKABLE_IMPRESSIONS,
                    ANNOTATION_CLOSABLE_IMPRESSIONS,
                    ANNOTATION_IMPRESSIONS,
                    CARD_IMPRESSIONS,
                    CARD_TEASER_IMPRESSIONS,
                    CALCULATED_CPM
                FROM ({query})
            )

            SELECT * FROM final_data
        """
