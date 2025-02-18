"""
* This Script is designed to execute in python environment on snowflake.
* This script takes raw data from Snowflake table (managed by A+E IT Team), processes that data and then stores output data back in Snowflake Tables.
* Key Features of Script:
    > If output table is not present in snowflake it automatically creates the tables in required schema.
    > It takes date range between last processed date from output table and latest available date from input table to determine range of data process.
      If complete month data is present then only data will be processed. Incomplete month data will not be processed.
    > All processed data will be appended in output tables.
    > Following output tables are created
        - POC.RSH_SOCIAL.YOUTUBE_MONTH_AGGREGATED > Powers DOMO dashboard
        - POC.RSH_SOCIAL.YOUTUBE_DAY_AGGREGATED > Powers DOMO dashboard
        - POC.RSH_SOCIAL.YOUTUBE_DEMOGRAPHICS     > Enhancement
        - POC.RSH_SOCIAL.YOUTUBE_TRAFFIC_SOURCE   > Enhancement
        - POC.RSH_SOCIAL.YOUTUBE_DEVICE_TYPE      > Enhancement
        - POC.RSH_SOCIAL.YOUTUBE_REGION_ROLLUP    > Enhancement
* This script is scheduled in snowflake procedure named : POC.RSH_SOCIAL.YT_DATA_PROCESS
* NOTE : If any changes are made in snowflake worksheet you need to deploy this script again and create or replace existing procedure for executing changes.
* Refer to given documentation for more details : https://aenetworks365.sharepoint.com/:w:/s/AdvancedDataSolutions/EaMDSDHAaPFOmST4l9p4ipIBHJqHhu5jY_gjiZDB42b3fw?e=t8eWaK
**
"""

# import libraries
import pandas as pd

# Import custom modules
from utils import utils
from video_id_data import Video_Id_Queries, Video_Id_TableCreator
from demo_data import DemographicsQueries, DemoTableCreator
from traffic_data import TrafficSourceQueries, TrafficTableCreator
from device_data import DeviceTypeQueries, DeviceTableCreator
from region_data import YouTubeRegionQueries, RegionTableCreator


class YouTubePipelineOrchestrator:
    """
    * This Class orchestrates all functions and executes them in required sequential manner to generate output.
    """

    def __init__(self, session) -> None:

        self.session = session

        # Initialize data processing classes
        self.video_id_data_queries = Video_Id_Queries(session)
        self.demo_data_queries = DemographicsQueries(session)
        self.traffic_data_queries = TrafficSourceQueries(session)
        self.device_data_queries = DeviceTypeQueries(session)
        self.region_queries = YouTubeRegionQueries(session)

        # Initialize table creater classes
        self.video_id_table_creater = Video_Id_TableCreator(session)
        self.demo_table_creater = DemoTableCreator(session)
        self.traffic_table_creater = TrafficTableCreator(session)
        self.device_table_creater = DeviceTableCreator(session)
        self.region_table_creater = RegionTableCreator(session)

        # Initiate utils class containing general functions
        self.utils = utils(session)

        # General table paths
        self.country_info_table = "POC.RSH_SOCIAL.YT_COUNTRY_INFO"
        self.YT_metadata = "AUDIENCE_DB.SMD.CONTENT_OWNER_VIDEO_METADATA"
        self.ppl_table = "BI_DB.BI.PPL_PROGRAM_VW"
        self.ppl_series_table = "BI_DB.BI.PPL_SERIES_VW"
        self.brand_table = "POC.RSH_SOCIAL.YT_BRAND_MAPPING"
        self.corrected_series_table = "POC.RSH_SOCIAL.YT_CORRECTED_SERIES"

        # Monthly video_id related table paths
        self.video_id_input_view_data_table = "AUDIENCE_DB.SMD.CONTENT_OWNER_BASIC"
        self.video_id_input_revenue_data_table = (
            "AUDIENCE_DB.SMD.CONTENT_OWNER_ESTIMATED_REVENUE"
        )
        self.monthly_video_id_output_table = "POC.RSH_SOCIAL.YOUTUBE_MONTH_AGGREGATED"
        self.monthly_video_id_domo_dataset_name = "YouTube_VideoId_monthly_fromSF"
        self.monthly_video_id_domo_id = "3769cce4-10ed-4aa7-8d5b-cc3996ad0221"

        # Daily video_id related paths
        self.daily_video_id_output_table = "POC.RSH_SOCIAL.YOUTUBE_DAY_AGGREGATED"
        self.daily_video_id_domo_dataset_name = "YouTube_Video_ID_Day_Aggregated"
        self.daily_video_id_domo_id = "0beab388-bc5f-42ef-805a-219758380cf1"

        # Demographics data related table paths
        self.demo_input_table = "AUDIENCE_DB.SMD.CONTENT_OWNER_DEMOGRAPHICS"
        self.demo_output_table = "POC.RSH_SOCIAL.YOUTUBE_DEMOGRAPHICS"
        self.demo_domo_dataset_name = "YouTube_Monthly_Demographics"
        self.demo_domo_id = "0c62a199-ac2c-4ec1-b68c-9b7ab3db8b33"

        # Traffic source data related paths
        self.traffic_input_table = "AUDIENCE_DB.SMD.CONTENT_OWNER_COMBINED"
        self.traffic_dim_table = "AUDIENCE_DB.SMD.DIM_YT_TRAFFIC_SOURCE"
        self.traffic_output_table = "POC.RSH_SOCIAL.YOUTUBE_TRAFFIC_SOURCE"
        self.traffic_domo_dataset_name = "YouTube_Traffic_Source_From_Snowflake"
        self.traffic_domo_id = "a15f759e-36a0-4c3b-bf20-cd672ad6ccc6"

        # Device type data related paths
        self.device_input_table = "AUDIENCE_DB.SMD.CONTENT_OWNER_COMBINED"
        self.device_dim_table = "AUDIENCE_DB.SMD.DIM_YT_DEVICE_TYPE"
        self.device_output_table = "POC.RSH_SOCIAL.YOUTUBE_DEVICE_TYPE"
        self.device_domo_dataset_name = "YouTube_Device_Type_From_Snowflake"
        self.device_domo_id = "2dea0d45-fdab-42fc-832b-fe8b39d55804"

        # Region related table paths
        self.region_output_table = "POC.RSH_SOCIAL.YOUTUBE_REGION_ROLLUP"
        self.region_domo_dataset_name = "YouTube_region_roll_up_monthly_fromSF"
        self.region_domo_id = "5992608f-39fc-47f6-b69c-c73708c321c6"

    def get_months_to_process(
        self, input_dataset: str, output_dataset: str, date_col: str
    ) -> tuple[str, str]:
        """
        Function:
            * Get the start and end dates to process data.
            * This function is used to process monthly data.

        Params:
            * input_dataset : str : Path of input dataset.
            * output_dataset : str : Path of output dataset.
            * date_col : str : Date column name in input dataset.

        Returns:
            * start_date in YYYYMMDD format
            * last_available_date in YYYYMMDD format

        """
        query = f"""
            WITH LastProcessed AS (
                SELECT 
                    TO_CHAR(
                        LAST_DAY(
                            TO_DATE(COALESCE(MAX("MONTH"), '2024-10-01'))
                        ), 'YYYYMMDD'
                    ) AS LAST_PROCESSED_DATE
                FROM 
                    {output_dataset}
            ),
            LatestAvailable AS (
                SELECT 
                    TO_CHAR(MAX({date_col})) AS LATEST_DATE_AVAILABLE
                FROM 
                    {input_dataset}
            ),
            DateRange AS (
                SELECT 
                    TO_DATE(LAST_PROCESSED_DATE, 'YYYYMMDD') AS START_DATE,
                    LATEST_DATE_AVAILABLE
                FROM 
                    LastProcessed, LatestAvailable
            )
            SELECT 
                TO_CHAR(ADD_MONTHS(TRUNC(START_DATE, 'MM'), 1), 'YYYYMMDD'),
                LATEST_DATE_AVAILABLE
            FROM 
                DateRange;
        """

        try:
            result = self.session.sql(query).collect()
            if not result:
                raise ValueError("No results returned from the query.")

            start_date = result[0][0]
            latest_date_available = result[0][1]

            # Validate dates
            if not start_date or not latest_date_available:
                raise ValueError(
                    f"Invalid dates returned: {start_date}, {latest_date_available}"
                )

            self.utils.log_message(
                f"Date range for {output_dataset} data processing: START_DATE = {start_date}, LATEST_DATE_AVAILABLE = {latest_date_available}"
            )

            return start_date, latest_date_available

        except Exception as e:
            self.utils.log_message(f"Error retrieving date range: {e}")
            raise

    def get_days_to_process(
        self,
        input_dataset: str,
        output_dataset: str,
        input_tb_date_col: str,
        output_tb_date_col: str,
    ) -> tuple[str, str]:
        """
        Function:
            * Get the start and end dates dynamically from the database.
            * This function is used to process daily data.
        Params:
            * input_dataset : str : Path of input dataset.
            * output_dataset : str : Path of output dataset.
            * input_tb_date_col : str : Date column name in input dataset.
            * output_tb_date_col : str : Date column name in input dataset.

        Returns:
            * start_date in YYYYMMDD format
            * last_available_date in YYYYMMDD format

        """
        query = f"""
            WITH LastProcessed AS (
                SELECT 
                    TO_CHAR(TO_DATE(COALESCE(DATEADD(DAY, 1, MAX({output_tb_date_col})), '2024-10-01')), 'YYYYMMDD') AS LAST_PROCESSED_DATE
                FROM 
                    {output_dataset}
            ),
            LatestAvailable AS (
                SELECT 
                    TO_CHAR(MAX({input_tb_date_col})) AS LATEST_DATE_AVAILABLE
                FROM 
                    {input_dataset}
            )
                SELECT 
                    LAST_PROCESSED_DATE,
                    LATEST_DATE_AVAILABLE
                FROM 
                    LastProcessed, LatestAvailable
            ;
        """

        try:
            result = self.session.sql(query).collect()

            start_date = result[0][0]
            latest_date_available = result[0][1]
            self.utils.log_message(
                f"Start_date = {start_date}, Latest_date_available = {latest_date_available}"
            )
            if start_date > latest_date_available:
                raise ValueError(
                    f"New data not available. Start Date: {start_date}, Last available date: {latest_date_available}"
                )

            else:
                return start_date, latest_date_available

        except Exception as e:
            self.utils.log_message(
                f"Error retrieving date range for daily process: {e}"
            )
            raise

    def get_month_start_end_dates(
        self, start_date: str, end_date: str
    ) -> list[tuple[str, str]]:
        """
        Function:
            * For monthly data process, get complete month start and end dates between start_date and end_date.

        Params:
            * start_date : str : Start date in 'YYYYMMDD' format.
            * end_date : str : End date in 'YYYYMMDD' format.

        Returns:
            * List of tuples containing start and end dates for each month.
        """
        # Convert start_date and end_date to datetime objects
        start_date_formatted = pd.to_datetime(start_date, format="%Y%m%d")
        end_date_formatted = pd.to_datetime(end_date, format="%Y%m%d")

        # Generate date ranges for the start and end of each month
        start_dates = pd.date_range(
            start=start_date_formatted, end=end_date_formatted, freq="MS"
        )
        end_dates = pd.date_range(
            start=start_date_formatted, end=end_date_formatted, freq="ME"
        )

        # Create a list of tuples with the start and end dates for each month
        month_dates = [
            (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
            for start, end in zip(start_dates, end_dates)
        ]

        return month_dates

    def get_month_end_date(self, start_date):
        """
        Function:
            * Get the last day of the month for the given start_date.

        Params:
            * start_date : str : Start date in 'YYYYMMDD' format.

        Returns:
            * month_end_date : str : Last day of the month in 'YYYYMMDD' format.

        """
        # Convert start_date to datetime object
        start_date = pd.to_datetime(start_date, format="%Y%m%d")

        # Get the last day of the month
        month_end_date = start_date + pd.offsets.MonthEnd(0)

        # Format the month end date to 'YYYYMMDD'
        month_end_date = month_end_date.strftime("%Y%m%d")

        return month_end_date

    def process_video_id_data(
        self,
        current_month_start_date: str,
        current_month_end_date: str,
        granularity: str,
    ) -> None:
        """
        Function:
            * This function acts as an orchestrator for transformation process for video_id data.
            * This function calls all necessary functions to process video_id data and inserts processed data in output table.
        Params:
            * current_month_start_date : First day of month to process.
            * current_month_end_date : Last day of month to process.
            * granularity : Aggregation granularity ('daily' or 'monthly').
        """
        try:
            self.utils.log_message(
                f"Processing data from {current_month_start_date} to {current_month_end_date} with granularity {granularity}"
            )
            self.utils.log_message(f"Video_ID data processing started.")

            yt_views_and_dims = self.video_id_data_queries.processed_YT_Views_and_dims(
                self.video_id_input_view_data_table,
                current_month_start_date,
                current_month_end_date,
            )

            yt_revenue_data = self.video_id_data_queries.processed_YT_revenue_data(
                self.video_id_input_revenue_data_table,
                current_month_start_date,
                current_month_end_date,
            )

            joined_data = self.video_id_data_queries.join_views_and_revenue_data(
                yt_views_and_dims, yt_revenue_data
            )

            if granularity == "monthly":
                aggregated_data = self.video_id_data_queries.aggregate_YT_data_monthly(
                    joined_data
                )
            elif granularity == "daily":
                aggregated_data = self.video_id_data_queries.aggregate_YT_data_daily(
                    joined_data
                )
            else:
                raise ValueError(
                    "Invalid granularity specified. Use 'daily' or 'monthly'."
                )

            video_title_mapped = self.utils.map_video_title(
                aggregated_data, self.YT_metadata
            )

            channel_mapped = self.utils.map_channel_names(
                video_title_mapped, self.YT_metadata
            )

            ppl_data = self.utils.extract_PPL_ID(self.YT_metadata, self.ppl_table)

            ppl_mapped_data = self.utils.map_PPL_data(
                channel_mapped, ppl_data, self.corrected_series_table
            )

            parent_series_id_mapped = self.utils.map_parent_series_id(
                ppl_mapped_data, self.ppl_table
            )

            parent_series_mapped = self.utils.map_parent_series_name(
                parent_series_id_mapped, self.ppl_series_table
            )

            brand_mapped_data = self.utils.map_brand_using_case_statement(
                parent_series_mapped
            )

            video_form_mapped_data = self.utils.map_video_form(brand_mapped_data)

            if granularity == "monthly":
                final_data = self.video_id_data_queries.reorder_table_columns(
                    video_form_mapped_data, "MONTH"
                )

                self.utils.insert_data_into_table(
                    self.monthly_video_id_output_table, final_data
                )

            elif granularity == "daily":
                final_data = self.video_id_data_queries.reorder_table_columns(
                    video_form_mapped_data, "DATE"
                )

                self.utils.insert_data_into_table(
                    self.daily_video_id_output_table, final_data
                )

            self.utils.log_message(
                f"Video_id Data for {current_month_start_date} to {current_month_end_date} processed successfully."
            )

        except Exception as e:
            self.utils.log_message(
                f"Error processing data for {current_month_start_date}: {e}"
            )

    def process_demo_data(
        self,
        current_month_start_date: str,
        current_month_end_date: str,
        granularity: str,
    ) -> None:
        """
        Function:
            * This function acts as an orchestrator for transformation process for Demographics data.
            * This function calls all necessary functions to process Demographics data and inserts processed data in output table.
        Params:
            * current_month_start_date : First day of month to process.
            * current_month_end_date : Last day of month to process.
            * granularity : Aggregation granularity ('daily' or 'monthly').
        """
        try:
            self.utils.log_message(
                f"Processing data from {current_month_start_date} to {current_month_end_date}"
            )
            self.utils.log_message(f"Demographics data processing started.")

            yt_views_and_dims = self.demo_data_queries.processed_YT_views_data(
                self.video_id_input_view_data_table,
                current_month_start_date,
                current_month_end_date,
            )

            get_demo_data = self.demo_data_queries.gather_demo_data(
                self.demo_input_table, current_month_start_date, current_month_end_date
            )

            joined_data = self.demo_data_queries.join_views_demo_data(
                yt_views_and_dims, get_demo_data
            )

            video_title_mapped = self.utils.map_video_title(
                joined_data, self.YT_metadata
            )

            channe_names_mapped = self.utils.map_channel_names(
                video_title_mapped, self.YT_metadata
            )

            video_form_mapped = self.utils.map_video_form(channe_names_mapped)

            group = ["UPLOADER_TYPE", "PLATFORM", "MONTH", "CHANNEL_ID", "CHANNEL_NAME"]
            channel_query = self.demo_data_queries.aggregate_data(
                video_form_mapped, group
            )

            group = [
                "UPLOADER_TYPE",
                "PLATFORM",
                "MONTH",
                "CHANNEL_ID",
                "CHANNEL_NAME",
                "VIDEO_FORM",
            ]
            video_form_query = self.demo_data_queries.aggregate_data(
                video_form_mapped, group
            )

            final_query = self.demo_data_queries.join_data(
                video_form_query, channel_query
            )

            self.utils.insert_data_into_table(self.demo_output_table, final_query)
            self.utils.log_message(
                f"Demographics Data for {current_month_start_date} to {current_month_end_date} processed successfully."
            )

        except Exception as e:
            self.utils.log_message(
                f"Error processing data for {current_month_start_date}: {e}"
            )

    def process_traffic_data(
        self,
        current_month_start_date: str,
        current_month_end_date: str,
        granularity: str,
    ) -> None:
        """
        Function:
            * This function acts as an otchestrator for transformation process for traffic source data.
            * This function calls all necessary functions to process traffic source data and inserts processed data in output table.
        Params:
            * current_month_start_date : First day of month to process.
            * current_month_end_date : Last day of month to process.
            * granularity : Aggregation granularity ('daily' or 'monthly').
        """
        try:
            self.utils.log_message(
                f"Processing data from {current_month_start_date} to {current_month_end_date}"
            )
            self.utils.log_message(f"Traffic Source data processing started.")

            yt_views_and_dims = self.traffic_data_queries.processed_YT_views_data(
                self.traffic_input_table,
                self.traffic_dim_table,
                current_month_start_date,
                current_month_end_date,
            )

            ppl_data = self.utils.extract_PPL_ID(self.YT_metadata, self.ppl_table)

            ppl_mapped_data = self.utils.map_PPL_data(
                yt_views_and_dims, ppl_data, self.corrected_series_table
            )

            parent_series_id_mapped = self.utils.map_parent_series_id(
                ppl_mapped_data, self.ppl_table
            )

            parent_series_name_mapped = self.utils.map_parent_series_name(
                parent_series_id_mapped, self.ppl_series_table
            )

            video_title_mapped = self.utils.map_video_title(
                parent_series_name_mapped, self.YT_metadata
            )

            channel_name_mapped = self.utils.map_channel_names(
                video_title_mapped, self.YT_metadata
            )

            brand_mapped_data = self.utils.map_brand_using_case_statement(
                channel_name_mapped
            )

            video_form_mapped = self.utils.map_video_form(brand_mapped_data)

            aggregated_data = self.traffic_data_queries.aggregate_data(
                video_form_mapped
            )

            self.utils.insert_data_into_table(
                self.traffic_output_table, aggregated_data
            )
            self.utils.log_message(
                f"Traffic Source Data for {current_month_start_date} to {current_month_end_date} processed successfully."
            )

        except Exception as e:
            self.utils.log_message(
                f"Error processing data for {current_month_start_date}: {e}"
            )

    def process_device_data(
        self,
        current_month_start_date: str,
        current_month_end_date: str,
        granularity: str,
    ) -> None:
        """
        Function:
            * This function acts as an otchestrator for transformation process for device type data.
            * This function calls all necessary functions to process device type data and inserts processed data in output table.
        Params:
            * current_month_start_date : First day of month to process.
            * current_month_end_date : Last day of month to process.
            * granularity : Aggregation granularity ('daily' or 'monthly').
        """
        try:
            self.utils.log_message(
                f"Processing data from {current_month_start_date} to {current_month_end_date}"
            )
            self.utils.log_message(f"Device type data processing started.")

            yt_views_and_dims = self.device_data_queries.processed_YT_views_data(
                self.device_input_table,
                self.device_dim_table,
                current_month_start_date,
                current_month_end_date,
            )

            video_title_mapped = self.utils.map_video_title(
                yt_views_and_dims, self.YT_metadata
            )

            channel_name_mapped = self.utils.map_channel_names(
                video_title_mapped, self.YT_metadata
            )

            video_form_mapped = self.utils.map_video_form(channel_name_mapped)

            brand_mapped = self.utils.map_brand_using_case_statement(video_form_mapped)

            aggregated_data = self.device_data_queries.aggregate_data(brand_mapped)

            final_data = self.device_data_queries.output_data(aggregated_data)

            self.utils.insert_data_into_table(self.device_output_table, final_data)
            self.utils.log_message(
                f"Device type Data for {current_month_start_date} to {current_month_end_date} processed successfully."
            )

        except Exception as e:
            self.utils.log_message(
                f"Error processing data for {current_month_start_date}: {e}"
            )

    def process_region_data(
        self,
        current_month_start_date: str,
        current_month_end_date: str,
        granularity: str,
    ) -> None:
        """
        Function:
            * This function acts as an otchestrator for transformation process for region data.
            * This function calls all necessary functions to process region data and inserts processed data in output table.
        Params:
            * current_month_start_date : First day of month to process.
            * current_month_end_date : Last day of month to process.
            * granularity : Aggregation granularity ('daily' or 'monthly').
        """
        try:
            self.utils.log_message(
                f"Processing data from {current_month_start_date} to {current_month_end_date}"
            )
            self.utils.log_message(f"region rollup data processing started.")

            yt_views_and_dims = self.region_queries.processed_YT_Views_and_dims(
                self.video_id_input_view_data_table,
                current_month_start_date,
                current_month_end_date,
            )

            yt_revenue_data = self.region_queries.processed_YT_revenue_data(
                self.video_id_input_revenue_data_table,
                current_month_start_date,
                current_month_end_date,
            )

            joined_data = self.region_queries.join_views_and_revenue_data(
                yt_views_and_dims, yt_revenue_data
            )

            video_title_mapped = self.utils.map_video_title(
                joined_data, self.YT_metadata
            )

            channel_name_mapped = self.utils.map_channel_names(
                video_title_mapped, self.YT_metadata
            )

            aggregated_data = self.utils.map_video_form(channel_name_mapped)

            final_data = self.region_queries.aggregate_YT_data(
                aggregated_data, self.country_info_table
            )

            self.utils.insert_data_into_table(self.region_output_table, final_data)
            self.utils.log_message(
                f"Region roll up Data for {current_month_start_date} to {current_month_end_date} processed successfully."
            )

        except Exception as e:
            self.utils.log_message(
                f"Error processing data for {current_month_start_date}: {e}"
            )

    def execute_domo_refresh(self) -> None:
        """
        Function:
            * Trigger DOMO dataset refresh using API

        Params:
            * sf_object_name : Object name in Snowflake database
            * domo_dataset_name : DOMO dataset name
            * domo_id : DOMO data unique ID
        """

        tables_info = self.get_output_tables_info()

        # For loop to execute data processing
        for key, value in tables_info.items():
            try:
                sf_object_name = value["output_table"]
                domo_dataset_name = value["domo_dataset_name"]
                domo_id = value["domo_id"]
            except Exception as e:
                self.utils.log_message(f"Error executing domo refresh function: {e}")

            try:
                query = f"""
                    CALL common_dev_db.automation_framework.DOMO_REFRESH('{{
                        "DATASET_TYPE": "Existing", 
                        "SF_OBJECT_NAME": "{sf_object_name}", 
                        "DOMO_DATASET_NAME": "{domo_dataset_name}", 
                        "DOMO_ID": "{domo_id}"
                    }}')
                """

                # Execute the SQL query using the session
                self.session.sql(query).collect()
            except Exception as e:
                self.utils.log_message(
                    f"Error occured while executing DOMO refresh: {e}"
                )

    def daily_view_refresh(self):
        try:
            daily_query = """CALL POC.RSH_SOCIAL.YT_DAILY_VIEW_CREATION()"""
            self.session.sql(daily_query).collect()
        except Exception as e:
            self.utils.log_message(
                f"Error occured while refreshing daily Snowflake views: {e}"
            )

    def monthly_view_refresh(self):
        try:
            monthly_query = """CALL POC.RSH_SOCIAL.YT_MONTHLY_VIEW_CREATION()"""
            self.session.sql(monthly_query).collect()
        except Exception as e:
            self.utils.log_message(
                f"Error occured while refreshing monthly Snowflake views: {e}"
            )

    def check_output_tables(self, table_name):
        """
        Function:
            * Checks if output table exists in Snowflake.
            * If the table does not exist, it raises an error.

        Params:
            * table_name : table path
        """
        db, schema, table = table_name.split(".")

        # Snowflake query to check if the table exists using SHOW TABLES
        query = f"""
            SHOW TABLES LIKE '{table}' IN SCHEMA {db}.{schema}
        """

        result = self.session.sql(query).collect()

        if not result:
            raise RuntimeError(
                f"Error: Table '{table_name}' does not exist in the database!"
            )

        self.utils.log_message(f"Table '{table_name}' already exists.")

    def get_output_tables_info(self):
        """
        function to add the required info for each output table:
        """
        # Dictionary of each output related variables
        output_tables_info = {
            "monthly_video_id": {
                "input_table": self.video_id_input_view_data_table,
                "output_table": self.monthly_video_id_output_table,
                "process_function": self.process_video_id_data,
                "domo_dataset_name": self.monthly_video_id_domo_dataset_name,
                "domo_id": self.monthly_video_id_domo_id,
                "date_column": "REPORTING_DATE",
            },
            "demo": {
                "input_table": self.demo_input_table,
                "output_table": self.demo_output_table,
                "process_function": self.process_demo_data,
                "domo_dataset_name": self.demo_domo_dataset_name,
                "domo_id": self.demo_domo_id,
                "date_column": "REPORTING_DATE",
            },
            "traffic": {
                "input_table": self.traffic_input_table,
                "output_table": self.traffic_output_table,
                "process_function": self.process_traffic_data,
                "domo_dataset_name": self.traffic_domo_dataset_name,
                "domo_id": self.traffic_domo_id,
                "date_column": "DATE",
            },
            "device": {
                "input_table": self.device_input_table,
                "output_table": self.device_output_table,
                "process_function": self.process_device_data,
                "domo_dataset_name": self.device_domo_dataset_name,
                "domo_id": self.device_domo_id,
                "date_column": "DATE",
            },
            "region": {
                "input_table": self.video_id_input_view_data_table,
                "output_table": self.region_output_table,
                "process_function": self.process_region_data,
                "domo_dataset_name": self.region_domo_dataset_name,
                "domo_id": self.region_domo_id,
                "date_column": "REPORTING_DATE",
            },
            "daily_video_id": {
                "input_table": self.video_id_input_view_data_table,
                "output_table": self.daily_video_id_output_table,
                "process_function": self.process_video_id_data,
                "domo_dataset_name": self.daily_video_id_domo_dataset_name,
                "domo_id": self.daily_video_id_domo_id,
                "date_column": "REPORTING_DATE",
            },
        }
        return output_tables_info

    def _regular_automated_execution(
        self, key, value, input_table, output_table, date_col, process_function
    ):
        """
        Function:
            * This function is used to process data without any reprocess.

        Params:
            * key : str : Key of the output table.
            * value : dict : Dictionary containing output table information.
            * input_table : str : Path of input table.
            * output_table : str : Path of output table.
            * date_col : str : Date column name in input table.
            * process_function : function : Function to process data.
        """
        # For loop to execute data processing
        # Processing daily_videoId data
        try:
            if key == "daily_video_id":
                start_day, end_day = self.get_days_to_process(
                    input_table, output_table, date_col, "DATE"
                )
                process_function(start_day, end_day, "daily")

                # Trigger daily snowflake view refresh procedure
                self.daily_view_refresh()

            # Processing all monthly data outputs
            else:
                start_date, latest_date_available = self.get_months_to_process(
                    input_table, output_table, date_col
                )

                months_to_process = self.get_month_start_end_dates(
                    start_date, latest_date_available
                )

                if not months_to_process:
                    self.utils.log_message(
                        f"No complete months to process for {key}. Start Date = {start_date}, Last Available Date = {latest_date_available}."
                    )
                else:
                    for month in months_to_process:
                        process_function(month[0], month[1], "monthly")

                    # Trigger monthly snowflake view refresh procedure
                    self.monthly_view_refresh()
        except Exception as e:
            raise ValueError(f"Error while executing automated process : {e}")

    def _manual_reprocess_execution(
        self,
        key,
        value,
        input_table,
        output_table,
        date_col,
        process_function,
        months_to_reprocess,
        is_reprocess_all,
    ):
        """
        Function:
            * This function is used to reprocess old month's data.
            * This function is still in development.

        Params:
            * key : str : Key of the output table.
            * value : dict : Dictionary containing output table information.
            * input_table : str : Path of input table.
            * output_table : str : Path of output table.
            * date_col : str : Date column name in input table.
            * process_function : function : Function to process data.
            * months_to_reprocess : str : Comma-separated list of first day of months to reprocess in 'YYYY-MM-DD' format.
            * is_reprocess_all : bool : Flag to indicate if all data after and including 'months_to_reprocess' date should be reprocessed.

        ⚠️ **NOTE:**
            * This function is still under development as of 2025-02-15. Do not use this function for now.
        """
        formatted_months = [month.strip() for month in months_to_reprocess.split(",")]
        formatted_months_yyyymmdd = [
            month.replace("-", "") for month in formatted_months
        ]
        min_month = min(formatted_months)

        if not is_reprocess_all and key != "daily_video_id":
            for month in formatted_months:
                last_day = self.session.sql(
                    f"SELECT TO_CHAR(LAST_DAY(TO_DATE('{month}', 'YYYY-MM-DD')))"
                ).collect()[0][0]
                self.utils.check_and_delete_data_old_data(
                    output_table, last_day, is_reprocess_all
                )

            for month_start_date in formatted_months_yyyymmdd:
                month_end_date = self.get_month_end_date(month_start_date)
                process_function(month_start_date, month_end_date, "monthly")

        elif is_reprocess_all and key != "daily_video_id":
            self.utils.check_and_delete_data_old_data(
                output_table, min_month, is_reprocess_all
            )
            start_date, latest_date_available = self.get_days_to_process(
                input_table, output_table, date_col, "MONTH"
            )
            months_to_process = self.get_month_start_end_dates(
                start_date, latest_date_available
            )
            for month in months_to_process:
                process_function(month[0], month[1], "monthly")

    def process_data(self, months_to_reprocess: str, is_reprocess_all: bool) -> None:
        """
        Function:
            * This function calls all data processing functions.

        Params:
            * months_to_reprocess : str : Comma-separated list of first day of months to reprocess in 'YYYY-MM-DD' format. example- '2024-12-01,2024-11-01'
            * is_reprocess_all : bool : Flag to indicate if all data after and including 'months_to_reprocess' date should be reprocessed.

        ⚠️ **NOTE:**
            * Currently months_to_reprocess is not supported. If input is provided for months_to_reprocess, it will raise an error.
            * We need to modify and finalize the logic to support months_to_reprocess in future for daily and monthly outputs.
        """

        if months_to_reprocess:
            raise ValueError(
                f"Found input for months_to_reprocess: {months_to_reprocess}. Stopping execution."
            )

        # get the output tables info
        tables_info = self.get_output_tables_info()

        # For loop to execute data processing
        for key, value in tables_info.items():
            try:
                input_table = value["input_table"]
                output_table = value["output_table"]
                date_col = value["date_column"]
                process_function = value["process_function"]

                self.utils.log_message(f"Processing for {key} data  started.")
                self.check_output_tables(output_table)

                # This section is for automated processing of data without any reprocess.
                if not months_to_reprocess:
                    self._regular_automated_execution(
                        key,
                        value,
                        input_table,
                        output_table,
                        date_col,
                        process_function,
                    )

                # This section is to reprocess old month's data. This section is still in devlopment.
                else:
                    self._manual_reprocess_execution(
                        key,
                        value,
                        input_table,
                        output_table,
                        date_col,
                        process_function,
                        months_to_reprocess,
                        is_reprocess_all,
                    )

            except Exception as e:
                self.utils.log_message(f"Error executing process_data function: {e}")

    def recreate_output_tables_if_not_present(self) -> None:
        """Create all necessary output tables if not already present.."""
        tables_and_sql = [
            self.video_id_table_creater.create_monthly_video_id_output_table_sql(
                self.monthly_video_id_output_table
            ),
            self.demo_table_creater.create_demo_output_table_sql(
                self.demo_output_table
            ),
            self.traffic_table_creater.create_traffic_output_table_sql(
                self.traffic_output_table
            ),
            self.device_table_creater.create_device_output_table_sql(
                self.device_output_table
            ),
            self.region_table_creater.create_region_output_table_sql(
                self.region_output_table
            ),
            self.video_id_table_creater.create_daily_video_id_output_table_sql(
                self.daily_video_id_output_table
            ),
        ]

        for create_sql in tables_and_sql:
            self.session.sql(create_sql).collect()


def run(session, MONTHS_TO_REPROCESS: str, IS_REPROCESS_ALL: bool):
    try:
        # Create utils instance
        utils_instance = utils(session)
        utils_instance.create_log_table()

        # Passing utils class in YouTubePipelineOrchestrator to execute its functions.
        processor = YouTubePipelineOrchestrator(session)

        # Recreate output tables if not already present. Uncomment when it's necessary to recreate output tables.
        # processor.recreate_output_tables_if_not_present()

        # start data processing. Keep MONTHS_TO_REPROCESS black as it is yet to implement logic to reprocess in script.
        processor.process_data(MONTHS_TO_REPROCESS, IS_REPROCESS_ALL)

        # Trigger domo dataset refresh using API call.
        # processor.execute_domo_refresh()

        # Log completion of processing loop
        utils_instance.log_message(f"Processing loop ended.")

        # Send notification email
        subject = "YouTube 2.0 Data Process Update"
        message = "Monthly data for YouTube 2.0 Pipeline Processed successfully. Please see processing log table below for more details."
        recipient = "advanceddatasolutions_devs@aenetworks.com"
        sender = "advanceddatasolutions_devs@aenetworks.com"
        closing_message = "Best Regards,"
        processing_log_query = "SELECT * FROM PROCESSING_LOG"

        # Send success notification email
        # utils_instance.send_email_notification(subject, message, recipient, sender, closing_message, processing_log_query)

    except Exception as e:
        # Log the error message and traceback
        utils_instance.log_message(f"Error occurred: {str(e)}")

        # Send notification email
        error_message = f"Error occurred in the Snowflake procedure. Details: {str(e)}"

        subject = "YouTube 2.0 Data Process Update"
        message = f"Data processing for YouTube 2.0 Pipeline failed. {error_message}. Please see processing log table below for more details."
        recipient = "advanceddatasolutions_devs@aenetworks.com"
        sender = "advanceddatasolutions_devs@aenetworks.com"
        closing_message = "Best Regards,"
        processing_log_query = "SELECT * FROM PROCESSING_LOG"

        # Send error notification email
        # utils_instance.send_email_notification(
        #     subject, message, recipient, sender, closing_message, processing_log_query
        # )

    finally:
        return session.table("PROCESSING_LOG")
