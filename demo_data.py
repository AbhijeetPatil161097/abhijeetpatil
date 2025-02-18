class DemoTableCreator:
    """
    This class creates output tables if not alredy present in Snowflake Database.
    """

    def __init__(self, session) -> None:
        self.session = session

    def create_demo_output_table_sql(self, demo_output_table: str) -> str:
        """
        Creates Demographics output table if already does not exist in required schema.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {demo_output_table} (
                PLATFORM VARCHAR(16777216),
                MONTH DATE,
                CHANNEL_ID VARCHAR(16777216),
                CHANNEL_NAME VARCHAR(16777216),
                VIDEO_FORM VARCHAR(16777216),
                UPLOADER_TYPE VARCHAR(20),
                VIEWS NUMBER(38,0),
                WATCH_TIME_MINUTES NUMBER(38,8),
                MEDIAN_AGE NUMBER(38,8),
                Percentage_Male NUMBER(38,8),
                Percentage_Female NUMBER(38,8),
                Percentage_Other NUMBER(38,8)
            );
        """


class DemographicsQueries:
    """
    This class contains all function unique to creating DEMOGRAPHICS output.
    """

    def __init__(self, session) -> None:
        self.session = session

    def processed_YT_views_data(
        self, demo_input_table: str, start_date: str, end_date: str
    ) -> str:
        """
        Function:
            Gather views data.
        """
        return f"""
        SELECT
            VIDEO_ID,
            REPORTING_DATE,
            CHANNEL_ID,
            UPLOADER_TYPE,
            CASE
                WHEN COUNTRY_CODE = 'US' THEN 'US' 
                ELSE 'Non-US'
            END AS GEO,
            SUM(VIEWS) AS VIEWS,
            SUM(WATCH_TIME_MINUTES) AS WATCH_TIME_MINUTES,
        FROM {demo_input_table}
        WHERE REPORTING_DATE BETWEEN {start_date} AND {end_date} AND UPLOADER_TYPE = 'self'
        GROUP BY ALL
        """

    def gather_demo_data(
        self, input_demo_table: str, start_date: str, end_date: str
    ) -> str:
        """
        Function:
            Aggregate processed data.

        Params:
            1. input_demo_table: Table for mapping demographics data (Gender, Age group)
            2. country_info_table: Country grouping mapping table.
            3. start_date: Start date from get_valid_date_range function.
            4. end_date: End date from get_valid_date_range function.

        Returns:
            Demographics data table
        """
        return f"""
            SELECT 
                REPORTING_DATE,
                VIDEO_ID,
                CHANNEL_ID,
                UPLOADER_CODE,
                CASE 
                    WHEN COUNTRY_CODE = 'US' THEN 'US' 
                    ELSE 'Non-US' 
                END AS GEO,
                GENDER,
                SUM(VIEW_PERCENTAGE/100) AS VIEW_PERCENTAGE_SUM,
                CASE 
                    WHEN AGE_GROUP = 'AGE_13_17' THEN 15 
                    WHEN AGE_GROUP = 'AGE_18_24' THEN 21
                    WHEN AGE_GROUP = 'AGE_25_34' THEN 30
                    WHEN AGE_GROUP = 'AGE_35_44' THEN 40
                    WHEN AGE_GROUP = 'AGE_45_54' THEN 50
                    WHEN AGE_GROUP = 'AGE_55_64' THEN 60
                    WHEN AGE_GROUP = 'AGE_65_' THEN 70
                END AS MID_AGE
            FROM 
                {input_demo_table}
            WHERE REPORTING_DATE BETWEEN {start_date} AND {end_date} AND UPLOADER_CODE = 'self'
            GROUP BY ALL
        """

    def join_views_demo_data(self, views_data: str, demo_data: str) -> str:
        """
        Function:
            1. Joins Views and Demographics Data.
            2. Aggregates data at channel_id and Month level.

        Params:
            1. views_data: Views data from function gather_views_data.
            2. demo_data: Demographics data from function gather_demo_data.

        Returns:
            Joined and aggregated dataset.
        """
        return f"""
            WITH joined_data AS (
                SELECT
                    'YouTube' AS PLATFORM,
                    COALESCE(a.REPORTING_DATE, b.REPORTING_DATE) AS REPORTING_DATE,
                    LAST_DAY(TO_DATE(TO_VARCHAR(b.REPORTING_DATE), 'YYYYMMDD')) AS MONTH,
                    COALESCE(a.VIDEO_ID, b.VIDEO_ID) AS VIDEO_ID,
                    COALESCE(a.CHANNEL_ID, b.CHANNEL_ID) AS CHANNEL_ID,
                    COALESCE(a.UPLOADER_TYPE, b.UPLOADER_CODE) AS UPLOADER_TYPE,
                    b.GENDER,
                    b.MID_AGE,
                    SUM(b.VIEW_PERCENTAGE_SUM * a.WATCH_TIME_MINUTES) AS DEMO_WATCH_TIME_MINUTES,
                    SUM(b.VIEW_PERCENTAGE_SUM * a.VIEWS) AS DEMO_VIEWS
                FROM ({views_data}) a
                RIGHT JOIN ({demo_data}) b
                    ON a.REPORTING_DATE = b.REPORTING_DATE
                    AND a.VIDEO_ID = b.VIDEO_ID
                    AND a.CHANNEL_ID = b.CHANNEL_ID

                    AND a.UPLOADER_TYPE = b.UPLOADER_CODE
                GROUP BY ALL
            )
            SELECT * FROM joined_data
        """

    def geo_views_sum(self, query: str) -> str:
        return f"""
            select
                LAST_DAY(TO_DATE(TO_VARCHAR(REPORTING_DATE), 'YYYYMMDD')) AS MONTH,
                CHANNEL_ID,
                GEO,
                SUM(DEMO_VIEWS) AS TOTAL_VIEWS,
                SUM(DEMO_WATCH_TIME_MINUTES) AS TOTAL_WATCH_TIME_MINUTES
            FROM 
                ({query})
            GROUP BY ALL
        """

    def aggregate_data(self, query: str, group_by_index: list) -> str:
        """
        calculate median age based on group by index
        """
        groupby_keys = ",".join(group_by_index)
        median_age_qry = f"""
        with input_tb as (
            {query}
        ),
        gender_views AS (
            select
                *,
                sum(demo_views) over(partition by {groupby_keys}) as total_views,
                case when gender ='MALE' then demo_views else 0 END as male_views,
                case when gender ='FEMALE' then demo_views else 0 END as female_views,
                case when gender ='USER_SPECIFIED' then demo_views else 0 END as other_views
            from input_tb
        ),
        demo_perc as (
            select
                {groupby_keys},
                mid_age,
                max(total_views) as total_views,
                sum(demo_views)/max(total_views) as demo_perc,
                sum(demo_views) as demo_views,
                sum(DEMO_WATCH_TIME_MINUTES) as TOTAL_WATCH_TIME_MINUTES,
                sum(male_views) as male_views,
                sum(female_views) as female_views,
                sum(other_views) as other_views
            from gender_views
            group by all
        ),
        median_age AS (
            select  
                {groupby_keys},
                -- max/min will be the same for total_views since groupbykey is same
                sum(mid_age * demo_perc) as median_age,
                sum(demo_views) as total_views,
                sum(TOTAL_WATCH_TIME_MINUTES) as TOTAL_WATCH_TIME_MINUTES,
                (sum(male_views)/max(total_views) * 100) as Percentage_Male,
                (sum(female_views)/max(total_views) * 100) as Percentage_Female,
                (sum(other_views)/max(total_views) * 100) as Percentage_Other,
            from demo_perc
            group by all
        )

        select * from median_age
        """
        return median_age_qry

    def join_data(self, video_form_query: str, channel_query: str) -> str:
        return f"""
                with video_form as (
                select
                    PLATFORM,
                    MONTH, 
                    CHANNEL_ID, 
                    CHANNEL_NAME, 
                    VIDEO_FORM,
                    UPLOADER_TYPE,
                    TOTAL_VIEWS as VIEWS,
                    TOTAL_WATCH_TIME_MINUTES AS WATCH_TIME_MINUTES,
                    MEDIAN_AGE,
                    PERCENTAGE_MALE,
                    PERCENTAGE_FEMALE,
                    PERCENTAGE_OTHER
                from ({video_form_query}) 
                ),
                channel as (
                select
                    PLATFORM,
                    MONTH, 
                    CHANNEL_ID, 
                    CHANNEL_NAME,
                    'ALL' AS VIDEO_FORM,
                    UPLOADER_TYPE,
                    TOTAL_VIEWS as VIEWS,
                    TOTAL_WATCH_TIME_MINUTES as WATCH_TIME_MINUTES,
                    MEDIAN_AGE,
                    PERCENTAGE_MALE,
                    PERCENTAGE_FEMALE,
                    PERCENTAGE_OTHER
                from ({channel_query})
                )
                select * from video_form
                union all
                select * from channel
            """
