from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, StringType

# --- Dimension Registry ---

_DIMENSIONS_REGISTRY: dict[str, DataFrame] = {}

def register_dimension(name: str, df: DataFrame):
    """Register a dimension DataFrame by name."""
    _DIMENSIONS_REGISTRY[name] = df

def get_dimension(name: str) -> DataFrame:
    """Retrieve a registered dimension DataFrame by name."""
    try:
        return _DIMENSIONS_REGISTRY[name]
    except KeyError:
        raise ValueError(f"Dimension '{name}' not found in registry")

# --- Helper functions ---

def join_and_filter(df: DataFrame, dim_df: DataFrame, join_expr, select_cols, filter_col: str) -> DataFrame:
    """
    Perform left join, select columns, and filter rows where filter_col is not null.
    """
    return df.join(dim_df, join_expr, "left") \
             .select(*select_cols) \
             .filter(col(filter_col).isNotNull())

def string_lookup(df: DataFrame, dim_df: DataFrame, left_col: str, right_col: str, output_col: str) -> DataFrame:
    """
    Join on lower(trimmed) columns and filter out rows where output_col is null.
    """
    return df.join(
        dim_df,
        lower(trim(df[left_col])) == lower(trim(dim_df[right_col])),
        "left"
    ).select(df["*"], dim_df[output_col]) \
     .filter(col(output_col).isNotNull())

def cast_columns(df: DataFrame, col_list: list[str], dtype) -> DataFrame:
    """
    Cast a list of columns to the specified dtype.
    """
    for col_name in col_list:
        df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df

# --- Main Processor Class ---

class QDataProcessor:
    """
    Class to process QData DataFrame with multiple dimension lookups and transformations.

    Attributes:
        df (DataFrame): Raw QData DataFrame to process.
        batch_id: Batch identifier for final tagging.
        logger: Optional logger for info/debug messages.
    """

    def __init__(self, QData_raw_df: DataFrame, batch_id, logger=None):
        if not isinstance(QData_raw_df, DataFrame):
            raise TypeError("QData_raw_df must be a pyspark.sql.DataFrame")
        if not isinstance(batch_id, (str, int)):
            raise TypeError("batch_id must be str or int")
        if logger and not callable(getattr(logger, "info", None)):
            raise ValueError("logger must have an info() method")

        self.df = QData_raw_df
        self.batch_id = batch_id
        self.logger = logger

    def _truncate_comments(self, df: DataFrame) -> DataFrame:
        if df.filter(length(col("Comment")) > 8000).limit(1).count() > 0:
            if self.logger:
                self.logger.info("Truncating comments longer than 8000 characters.")
            return df.withColumn("Comment", substring(col("Comment"), 1, 8000))
        return df

    def _convert_percentages(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "Measure_Value",
            when(col("Measure_Value").contains("%"),
                 regexp_replace("Measure_Value", "%", "").cast("float") / 100)
            .otherwise(col("Measure_Value"))
        )

    def _join_company(self, df: DataFrame) -> DataFrame:
        company_dim_df = get_dimension("company_dim")
        joined_df = df.join(
            company_dim_df,
            (df.Organisation_Cd == company_dim_df.Organisation_Cd) &
            (company_dim_df["Current_Ind"] == 'Y'),
            "left"
        ).select(
            df["*"],
            company_dim_df["Company_Instance_Id"],
            company_dim_df["Company_Id"]
        ).drop(
            "Template_Version", "Sheet_Cd", "Submission_Date", "Process_Cd",
            "Measure_Name", "Measure_Desc", "Measure_Area"
        ).filter(
            col("Company_Instance_Id").isNotNull() & col("Company_Id").isNotNull()
        )
        return joined_df

    def _join_region(self, df: DataFrame) -> DataFrame:
        region_dim_df = get_dimension("region_dim")

        df_na = join_and_filter(
            df.filter(col("Region_Cd") == 'NA'),
            region_dim_df,
            df["Region_Cd"] == region_dim_df["Region_Cd"],
            [df["*"], region_dim_df["Region_Id"]],
            "Region_Id"
        )

        df_valid = join_and_filter(
            df.filter(col("Region_Cd") != 'NA'),
            region_dim_df,
            (df["Organisation_Cd"] == region_dim_df["Organisation_Cd"]) &
            (df["Region_Cd"] == region_dim_df["Region_Cd"]),
            [df["*"], region_dim_df["Region_Id"]],
            "Region_Id"
        )

        return df_na.union(df_valid)

    def _assign_assurance_id(self, df: DataFrame) -> DataFrame:
        map_assurance_dim_df = get_dimension("map_assurance_dim")
        assurance_dim_df = get_dimension("assurance_dim")

        assurance_row = map_assurance_dim_df \
            .filter(col("Collection_Process") == "QD") \
            .join(assurance_dim_df, "Assurance_Level", "left") \
            .select("Assurance_Id") \
            .limit(1).collect()

        if not assurance_row:
            raise RuntimeError("No Assurance_Id found for Collection_Process='QD'")

        assurance_id = assurance_row[0]["Assurance_Id"]

        df_with_assurance = df.withColumn("Assurance_Id", lit(assurance_id))
        df_filtered = df_with_assurance.filter(col("Assurance_Id").isNotNull())
        if df_filtered.count() < df.count():
            if self.logger:
                self.logger.info("Some rows dropped due to null Assurance_Id")
        return df_filtered

    def _join_sensitivity(self, df: DataFrame) -> DataFrame:
        sensitivity_dim_df = get_dimension("sensitivity_dim")
        return string_lookup(df, sensitivity_dim_df, "Security_Mark", "Security_Mark", "Sensitivity_Id")

    def _map_measure(self, df: DataFrame) -> DataFrame:
        map_measure_dim_df = get_dimension("map_measure_dim")
        measure_dim_df = get_dimension("measure_dim")

        df = join_and_filter(
            df,
            map_measure_dim_df,
            (df["Measure_Cd"] == map_measure_dim_df["Legacy_BonCode"]) &
            (map_measure_dim_df["Current_Ind"] == 'Y'),
            [df["*"], map_measure_dim_df["Ocean_Measure_Code"]],
            "Ocean_Measure_Code"
        )

        df = join_and_filter(
            df,
            measure_dim_df,
            (df["Ocean_Measure_Code"] == measure_dim_df["Measure_Cd"]) &
            (measure_dim_df["Current_Ind"] == 'Y'),
            [
                df["*"],
                measure_dim_df["Measure_Instance_Id"],
                measure_dim_df["Measure_Id"],
                measure_dim_df["Decimal_Point"],
                measure_dim_df["Unit"]
            ],
            "Measure_Id"
        )

        df = df.withColumn(
            "Decimal_Point",
            when(col("Unit") == "%", col("Decimal_Point") + 2).otherwise(col("Decimal_Point"))
        )

        df = df.withColumn(
            "Measure_Value",
            when(col("Unit") != 'hh:mm:ss',
                 round(col("Measure_Value") * pow(10, col("Decimal_Point"))) /
                 pow(10, col("Decimal_Point")))
            .otherwise(col("Measure_Value"))
        )

        return df

    def _join_observations(self, df: DataFrame) -> DataFrame:
        observation_coverage_dim_df = get_dimension("observation_coverage_dim")
        observation_dim_df = get_dimension("observation_dim")

        df = string_lookup(df, observation_coverage_dim_df, "Observation_Coverage_Desc", "Observation_Coverage_Desc", "Observation_Coverage_Id")
        df = string_lookup(df, observation_dim_df, "Observation_Desc", "Observation_Desc", "Observation_Id")
        return df

    def _join_intervals(self, df: DataFrame) -> DataFrame:
        interval_dim_df = get_dimension("interval_dim")

        df = join_and_filter(
            df,
            interval_dim_df,
            df["Submission_Period_Cd"] == interval_dim_df["Interval_cd"],
            [df["*"], interval_dim_df["Interval_Id"].alias("Submission_Period_Id")],
            "Submission_Period_Id"
        )

        interval_obs_df = interval_dim_df.withColumnRenamed("Interval_Id", "Observation_Period_Id")

        df = join_and_filter(
            df,
            interval_obs_df,
            df["Observation_Period_Cd"] == interval_obs_df["Interval_cd"],
            [df["*"], interval_obs_df["Observation_Period_Id"]],
            "Observation_Period_Id"
        )

        return df

    def _join_data_source(self, df: DataFrame) -> DataFrame:
        data_source_dim_df = get_dimension("data_source_dim")
        return string_lookup(df, data_source_dim_df, "Data_Source", "Data_Source_Desc", "Data_Source_Id")

    def _finalize(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "Measure_Value",
            when(col("Measure_Value").isNull(), "---").otherwise(col("Measure_Value"))
        )

        df = df.drop("Cell_Cd") \
               .withColumnRenamed("Comment", "Measure_Comment") \
               .withColumnRenamed("Measure_Cd", "Legacy_Measure_Reference")

        df = cast_columns(df, [
            "Company_Instance_Id", "Company_Id", "Measure_Id", "Assurance_Id",
            "Sensitivity_Id", "Observation_Id", "Region_Id", "Observation_Coverage_Id",
            "Data_Source_Id", "Measure_Instance_Id"
        ], LongType())

        df = df.withColumn("Measure_Value", col("Measure_Value").cast(StringType()))
        df = df.withColumn("Batch_Id", lit(self.batch_id).cast(StringType()))
        df = df.withColumn("Insert_Date", current_timestamp())
        df = df.withColumn("Update_Date", lit(None).cast("timestamp"))
        return df

    def process(self) -> DataFrame:
        df = self.df
        df = self._truncate_comments(df)
        df = self._convert_percentages(df)
        df = self._join_company(df)
        df = self._join_region(df)
        df = self._assign_assurance_id(df)
        df = self._join_sensitivity(df)
        df = self._map_measure(df)
        df = self._join_observations(df)
        df = self._join_intervals(df)
        df = self._join_data_source(df)
        df = self._finalize(df)
        return df
