class DeviceTableCreator:
    """
    This class creates output tables if not alredy present in Snowflake Database.
    """

    def __init__(self, session) -> None:
        self.session = session

    def create_device_output_table_sql(self, device_output_table: str) -> str:
        """
        Creates Device Type output table if already does not exist in required schema.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {device_output_table} (
                PLATFORM VARCHAR(7),
                MONTH DATE,
                CHANNEL_ID VARCHAR(16777216),
                CHANNEL_NAME VARCHAR(16777216),
                DEVICE_TYPE_NAME VARCHAR(50),
                UPLOADER_TYPE VARCHAR(50),
                US_NON_US VARCHAR(6),
                VIEWS NUMBER(38,0),
                WATCH_TIME_MINUTES NUMBER(38,8),
                VIDEO_FORM VARCHAR(50),
                BRAND_ROLL_UP VARCHAR(50)
            );
        """


class DeviceTypeQueries:
    """
    This class contains all function unique to creating device type output.
    """

    def __init__(self, session) -> None:
        self.session = session

    def processed_YT_views_data(
        self,
        input_dataset: str,
        device_type_dim_table: str,
        start_date: str,
        end_date: str,
    ) -> str:
        """
        Function:
            Gather YouTube views metadata from given date range.
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
                b.DEVICE_TYPE_NAME,
                SUM(a.VIEWS) AS VIEWS,
                SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES
            FROM 
                {input_dataset} a
            JOIN 
                {device_type_dim_table} b ON a.DEVICE_TYPE = b.DEVICE_TYPE
            WHERE 
                a.DATE BETWEEN '{start_date}' AND '{end_date}' AND a.UPLOADER_TYPE = 'self'
            GROUP BY
                ALL
        """

    def aggregate_data(self, device_type_query: str) -> str:
        return f"""
            SELECT 
                a.PLATFORM,
                LAST_DAY(TO_DATE(TO_VARCHAR(a.DATE), 'YYYYMMDD')) AS MONTH,
                a.CHANNEL_ID,
                a.CHANNEL_NAME,
                CASE
                    WHEN DEVICE_TYPE_NAME IN ('Mobile Phone', 'Tablet') THEN 'Mobile Phone & Tablet'
                    WHEN DEVICE_TYPE_NAME IN ('Computer', 'TV') THEN DEVICE_TYPE_NAME
                    ELSE 'Other'
                END AS DEVICE_TYPE_NAME,
                a.UPLOADER_TYPE,
                a.US_NON_US,
                SUM(a.VIEWS) AS VIEWS,
                SUM(a.WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
                a.VIDEO_FORM,
                a.BRAND_ROLL_UP
            FROM ({device_type_query}) a
            GROUP BY
                a.PLATFORM,
                MONTH,
                a.CHANNEL_ID,
                a.UPLOADER_TYPE,
                a.DEVICE_TYPE_NAME,
                a.US_NON_US,
                a.CHANNEL_NAME,
                a.BRAND_ROLL_UP,
                a.VIDEO_FORM
        """

    def output_data(self, query: str) -> str:
        return f"""
            select
                PLATFORM,
                MONTH,
                CHANNEL_ID,
                CHANNEL_NAME,
                DEVICE_TYPE_NAME,
                UPLOADER_TYPE,
                US_NON_US,
                SUM(VIEWS) AS VIEWS,
                SUM(WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
                VIDEO_FORM,
                BRAND_ROLL_UP
            from
                ({query})
            GROUP BY 
                ALL
        """
