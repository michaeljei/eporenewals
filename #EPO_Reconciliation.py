#EPO_Reconciliation

import logging
import datetime
import pyspark
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

#Parameters

quarterly = True
epo_renewal_pct_split = 50
interest_due_year3 = 0.00
interest_due_year4 = 0.00
silver_datalake = "" #stuksee3fsbdatmibisilver.dfs.core.windows.net
date_today = "" #2025-01-03
notebook_status = True
try:
    fee_path = f"abfss://renewals@{silver_datalake}/epo_minimum_fees.csv"
    fee_amounts_df = spark.read.csv(fee_path, header = True)
except Exception as excp:
    _logger.error(f"Error retrieving mimnimum fee file {excp}.")
    notebook_status = False
try:
    date_today = datetime.datetime.strptime(date_today, "%Y-%m-%d")

    if quarterly:
        end_date = date_today.replace(day=1) - datetime.timedelta(days=1)
        start_date = end_date.replace(day=1, month=end_date.month - 2 if end_date.month > 2 else 12, year=end_date.year - (1 if end_date.month == 1 else 0))
    else:
        end_date = date_today.replace(day=1) - datetime.timedelta(days=1)
        start_date = end_date.replace(day=1)
        
    end_date = end_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
except Exception as excp:
    _logger.error(f"Error defining start and end date ranges: {excp}.")
    notebook_status = False

#Queries
#Renewal

def get_new_renewals_dataframe():
    crm_renewals_query = """
        SELECT 
            IPR.ipo_applicationid AS application_num,
            IPR.ipo_publicationid AS publication_num,
            IPR.ipo_applicationdate AS filing_date,
            IPR.ipo_bpublicationdate AS grant_date,
            IPR.ipo_renewalyear AS renewal_year,
            F.ipo_totalfee AS renewal_fee, 
            IPR.ipo_nextrenewaldate AS next_renewal_date,
            I.ipo_datefiled AS date_filed,
            I.ipo_datecompleted AS date_processed
        FROM crm_ldb.ipo_incident_iprights IR
        LEFT JOIN crm_ldb.incident I ON I.incidentid = IR.incidentid
        LEFT JOIN crm_ldb.ipo_servicerequest SR ON I.ipo_servicerequestid = SR.Id
        LEFT JOIN crm_ldb.ipo_ipright IPR ON IPR.ipo_iprightid = IR.ipo_iprightId
        LEFT JOIN crm_ldb.ipo_fees F ON IPR.ipo_iprightid = F.ipo_rightid
        LEFT JOIN crm_ldb.ipo_feeitems FI ON F.ipo_feesid = FI.ipo_feeid
        WHERE SR.ipo_servicerequesttypeidname = 'Renewals'
    """
    return spark.sql(crm_renewals_query)

def get_legacy_renewals_dataframe():
    cops_renewals_query = """
        SELECT 
            FK_I4300_CASE_KEY AS application_num,
            FK_PS_4000_4300_PK_I4000_CS_KY AS publication_num,
            I4300_FILING_DATE_Expanded AS filing_date,
            I4300_GRANT_DATE_Expanded AS grant_date,
            I4330_LAST_RENEWAL_YEAR AS renewal_year,
            I4330_FEE_TAKEN AS renewal_fee,
            I4330_EXTENSION_FEE_TAKEN AS late_fee,
            I4330_EXTENSION_MONTH AS late_months,
            I4300_RENEWAL_DATE_Expanded AS next_renewal_date,
            I4330_FILING_DATE_Expanded AS date_filed,
            I4330_ACTIONED_DATE_Expanded AS date_processed
        FROM cops_ldb.PR4330FORMSLOG F
        LEFT JOIN cops_ldb.PR4300STANDING S
        ON F.FK_I4300_CASE_KEY = S.PK_I4300_CASE_KEY
        WHERE I4330_FORM_TYPE_NUM = 12
    """
    return spark.sql(cops_renewals_query)

#Restoration

def get_legacy_restored_dataframe():
    cops_restore_query = """
        SELECT
            PK_I4300_CASE_KEY AS application_num,
            FK_PS_4000_4300_PK_I4000_CS_KY AS publication_num,
            I4300_FILING_DATE_Expanded AS filing_date,
            I4300_GRANT_DATE_Expanded AS grant_date,
            I4330_FILING_DATE_Expanded AS date_filed,
            I4300_RESTORE_ALLOWED_DATE_Expanded AS date_processed
        FROM cops_ldb.PR4300STANDING S
        LEFT JOIN cops_ldb.PR4330FORMSLOG F
        ON S.PK_I4300_CASE_KEY = F.FK_I4300_CASE_KEY
        WHERE I4300_RESTORE_DATE_Expanded IS NOT NULL
        AND I4330_FORM_TYPE_NUM = 16
    """
    return spark.sql(cops_restore_query)

def get_new_restored_dataframe():
    mds_restore_query = """
        SELECT 
            ApplicationNumber AS application_num,
            PublicationNumber AS publication_num,
            LegalFilingDate AS filing_date,
            BPublicationDate AS grant_date,
            CaseCreateDate AS date_filed,
            RestorationDate AS date_processed
        FROM mds_ldb.Right R
        LEFT JOIN mds_ldb.Patent P ON R.RightId = P.RightId
        LEFT JOIN mds_ldb.RightCase RC ON R.RightId = RC.RightId
        LEFT JOIN mds_ldb.Case C On RC.CaseId = C.CaseId
        WHERE RestorationDate IS NOT NULL
        AND CaseCategoryId = 31
    """
    return spark.sql(mds_restore_query)

#Licence of Right

def get_legacy_lor_dataframe():
    cops_lor_cols = [
        "PK_I4300_CASE_KEY AS application_num",
        "TRIM(FK_PS_4000_4300_PK_I4000_CS_KY) AS publication_num",
        "I4300_FILING_DATE_Expanded AS filing_date,"
        "I4300_GRANT_DATE_Expanded AS grant_date",
        "I4330_FILING_DATE_Expanded AS date_filed"
    ]
    cops_lor_col_string = ",".join(cops_lor_cols)
    
    cops_lor_query = f"""
        SELECT {cops_lor_col_string}, 
        COALESCE(I4330_ACTIONED_DATE_Expanded, I4300_DATE_OF_LOR_Expanded) AS date_processed, 'A' AS Addition_Deletion
        FROM cops_ldb.PR4300STANDING S
        LEFT JOIN cops_ldb.PR4330FORMSLOG F
        ON S.PK_I4300_CASE_KEY = F.FK_I4300_CASE_KEY
        WHERE I4330_FORM_TYPE_NUM = 28
        AND (I4330_ACTIONED_DATE_Expanded IS NOT NULL OR I4300_DATE_OF_LOR_Expanded IS NOT NULL)
        UNION
        SELECT {cops_lor_col_string}, 
        I4300_DATE_LOR_CANCEL_Expanded AS date_processed, 'D' AS Addition_Deletion
        FROM cops_ldb.PR4300STANDING S
        LEFT JOIN cops_ldb.PR4330FORMSLOG F
        ON S.PK_I4300_CASE_KEY = F.FK_I4300_CASE_KEY
        WHERE I4330_FORM_TYPE_NUM = 30
        AND I4300_DATE_LOR_CANCEL_Expanded IS NOT NULL
    """
    return spark.sql(cops_lor_query)

def get_new_lor_dataframe():
    mds_lor_cols = [
        "ApplicationNumber AS application_num",
        "PublicationNumber AS publication_num",
        "BPublicationDate AS grant_date",
        "LegalFilingDate AS filing_date",
        "CaseCreateDate AS date_filed"
    ]
    mds_lor_col_string = ",".join(mds_lor_cols)
    
    mds_lor_query = f"""
        SELECT {mds_lor_col_string}, 
        LORGrantedDate AS date_processed, 'A' AS Addition_Deletion
        FROM mds_ldb.LicenceOfRight L
        LEFT JOIN mds_ldb.Right R ON L.RightId = R.RightId
        LEFT JOIN mds_ldb.Patent P ON L.RightId = P.RightId
        LEFT JOIN mds_ldb.RightCase RC ON R.RightId = RC.RightId
        LEFT JOIN mds_ldb.Case C On RC.CaseId = C.CaseId
        WHERE LORGrantedDate IS NOT NULL AND CaseCategoryId = 17
        UNION
        SELECT {mds_lor_col_string}, 
        LORCancelledDate AS date_processed, 'D' AS Addition_Deletion
        FROM mds_ldb.LicenceOfRight L
        LEFT JOIN mds_ldb.Right R ON L.RightId = R.RightId
        LEFT JOIN mds_ldb.Patent P ON L.RightId = P.RightId
        LEFT JOIN mds_ldb.RightCase RC ON R.RightId = RC.RightId
        LEFT JOIN mds_ldb.Case C On RC.CaseId = C.CaseId
        WHERE LORCancelledDate IS NOT NULL AND CaseCategoryId = 17
    """
    return spark.sql(mds_lor_query)

#Lapse of Right

def get_legacy_lapsed_dataframe():
    cops_lapse_query = f"""
        SELECT TRIM(FK_PS_4000_4300_PK_I4000_CS_KY) AS publication_num,
               I4300_FILING_DATE_Expanded AS filing_date,
               CASE WHEN I4300_REASON_NOT_IN_FORCE = 00 THEN 'Ceased'
                WHEN I4300_REASON_NOT_IN_FORCE = 04 THEN 'Expired'
                WHEN I4300_REASON_NOT_IN_FORCE = 05 THEN 'Surrendered'
                WHEN I4300_REASON_NOT_IN_FORCE = 07 THEN 'Revoked'
                END AS lapsed_reason,
               I4300_DATE_NOT_IN_FORCE_Expanded AS lapsed_date,
               I4340_EVENT_DATE_Expanded AS date_processed
        FROM cops_ldb.PR4300STANDING S
        LEFT JOIN cops_ldb.PR4340REGHIST H
        ON S.PK_I4300_CASE_KEY = H.FK_I4300_CASE_KEY
        WHERE I4340_REG_DATE_Expanded BETWEEN '{start_date}' AND '{end_date}'
        AND FK_I4300_CASE_KEY LIKE 'EP%'
        AND I4300_DATE_NOT_IN_FORCE_Expanded IS NOT NULL
        AND I4300_DATE_NOT_IN_FORCE_Expanded = I4340_EVENT_DATE_Expanded
        AND I4300_REASON_NOT_IN_FORCE IN ('00', '04', '05', '07')
    """
    return spark.sql(cops_lapse_query)

def get_new_lapsed_dataframe():
    mds_lapse_query = f"""
        SELECT PublicationNumber AS publication_num,
               LegalFilingDate AS filing_date,
               TerminationReasonDescription AS lapsed_reason,
               TerminationDate AS lapsed_date,
               EventDate AS date_processed
        FROM mds_ldb.Case C 
        LEFT JOIN mds_ldb.CaseCategory CC ON C.CaseCategoryId = CC.CaseCategoryId
        LEFT JOIN mds_ldb.RightCase RC ON C.CaseId = RC.CaseId
        LEFT JOIN mds_ldb.RegisterEvent RE ON RC.RightCaseId = RE.RightCaseId
        LEFT JOIN mds_ldb.Right R ON RC.RightId = R.RightId
        LEFT JOIN mds_ldb.Patent P ON R.RightId = P.RightId
        LEFT JOIN mds_ldb.TerminationReason T ON R.TerminationReasonId = T.TerminationReasonId
        WHERE EventDate BETWEEN '{start_date}' AND '{end_date}'
        AND PublicationNumber LIKE 'EP%'
        AND CaseCategory IN ('Cessation', 'Expiry', 'Surrender', 'Revocation')
        AND RegisterIndicator = 1
    """
    return spark.sql(mds_lapse_query)

#Functions

@f.udf(returnType=ArrayType(StringType()))
def format_fee_data_udf(fee: float):
    """
    Formats the fee data into a specific string format.

    Arguments:
        fee: Float representing the fee amount to be formatted.

    Returns:
        list: List containing the formatted fee as two separate strings (whole part and hundredths part).
    """
    fee = f"{fee:011.2f}"
    fee_whole, fee_hundredths = fee.split(".")
    return [fee_whole, fee_hundredths]

def combine_dataframes(df1: pyspark.sql.DataFrame, df2: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Combines two DataFrames by aligning their columns and performing a union.

    Arguments:
        df1: First PySpark DataFrame to be combined.
        df2: Second PySpark DataFrame to be combined.

    Returns:
        pyspark.sql.DataFrame: Combined PySpark DataFrame with columns from both input DataFrames.
    """
    combined_columns = list(set(df1.columns).union(set(df2.columns)))
    
    for col in combined_columns:
        if col not in df1.columns:
            df1 = df1.withColumn(col, f.lit(None))
        if col not in df2.columns:
            df2 = df2.withColumn(col, f.lit(None))
    
    combined_df = df1.unionByName(df2)
    return combined_df

def calculate_renewal_fee_splits(
    renewals_df: pyspark.sql.DataFrame, 
    fee_amounts_df: pyspark.sql.DataFrame,
    epo_renewal_pct_split: int, 
    interest_due_year3: float, 
    interest_due_year4: float
) -> pyspark.sql.DataFrame:
    """
    Calculates the renewal fee splits and updates the DataFrame with the calculated values.

    Arguments:
        renewals_df: PySpark DataFrame containing the renewal data.
        fee_amounts_df: PySpark DataFrame containing the historic minimum fees.
        epo_renewal_pct_split: Integer representing the EPO renewal percentage split.
        interest_due_year3: Float representing the interest due for year 3.
        interest_due_year4: Float representing the interest due for year 4.

    Returns:
        pyspark.sql.DataFrame: Updated PySpark DataFrame with the calculated renewal fee splits.
    """
    interest_due = interest_due_year3 + interest_due_year4
    epo_renewal_pct_split = epo_renewal_pct_split / 100

    fee_amounts_df = fee_amounts_df.filter(f.col("minimum_fee_year").isin([3, 4]))
    renewals_with_fee_split_df = renewals_df
    for year in [3, 4]:
        renewals_with_fee_split_df = renewals_with_fee_split_df.withColumn(
            f"renewal_due_date_yr{year}", f.year(f.col("renewal_due_date")) - (5 - year)
        )
        
        renewals_with_fee_split_df = renewals_with_fee_split_df.join(
            fee_amounts_df.filter(f.col("minimum_fee_year") == year),
            renewals_with_fee_split_df[f"renewal_due_date_yr{year}"] == fee_amounts_df.year_set,
            "left"
        ).withColumn(
            f"renewal_fee_year{year}", f.when(
                (f.col("grant_date") <= f.add_months(f.col("filing_date"), 12 * (year - 1))) & (f.col("renewal_year") == 5),
                f.col("minimum_fee_amount").cast("double")
            ).otherwise(f.lit(0.00)).cast("double")
        ).drop(f"renewal_due_date_yr{year}", "minimum_fee_year", "year_set", "minimum_fee_amount")

    renewals_with_fee_split_df = renewals_with_fee_split_df \
        .withColumn("renewal_fee", f.col("renewal_fee").cast("double")) \
        .withColumn("renewal_fee_epo", f.round(f.col("renewal_fee") * epo_renewal_pct_split, 2)) \
        .withColumn("total_epo_amount", f.round(f.expr(f"renewal_fee_epo + renewal_fee_year3 + renewal_fee_year4 + {interest_due}"),2))
    
    return renewals_with_fee_split_df

def calculate_renewal_due_dates(renewals_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Calculate the due date when renewal was paid and update the DataFrame.

    Arguments:
        renewals_df: PySpark DataFrame containing the renewal data.

    Returns:
        pyspark.sql.DataFrame: Updated PySpark DataFrame with the calculated renewal due date.
    """
    renewals_df_with_due_date = renewals_df.withColumn(
    "renewal_due_date",
    f.add_months(f.col("filing_date"), (f.col("renewal_year")-1)*12)
    )
    renewals_df_with_due_date = renewals_df_with_due_date.withColumn(
        "renewal_due_date",
        f.when(
            (f.col("grant_date") >= (f.col("renewal_due_date") - f.expr("interval 3 months"))) &
            (f.col("grant_date") <= f.col("renewal_due_date")),
            f.col("grant_date") + f.expr("interval 3 months")
        ).otherwise(f.col("renewal_due_date"))
    )

    window_spec = Window.partitionBy("application_num", "date_filed")
    renewals_df_multiples = renewals_df_with_due_date.withColumn(
        "multiple_renewals",
        (f.count("date_filed").over(window_spec) > 1).cast("boolean")
    ).withColumn(
        "renewal_due_date",
        f.when(f.col("multiple_renewals"), f.add_months(f.col("renewal_due_date"), 12)).otherwise(f.col("renewal_due_date"))
    ).drop("multiple_renewals", "next_renewal_date")

    return renewals_df_multiples

def generate_header_footer_information() -> tuple:
    """
    Generates header and footer information for the EPO output.

    Returns:
        tuple: Tuple containing two DataFrames: header_df and footer_df.
    """
    def format_fee_totals(fee):
        fee = f"{fee:016.2f}"
        fee_whole, fee_hundredths = fee.split(".")
        return [fee_whole, fee_hundredths]

    def format_date(date_str):
        return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%y')

    start_date_formatted = format_date(start_date)
    end_date_formatted = format_date(end_date)
    date_today_formatted = date_today.strftime('%d%m%y')
    ipo_total_whole, ipo_total_hundredths = format_fee_totals(grand_total_prescribed)
    epo_total_whole, epo_total_hundredths = format_fee_totals(grand_total_remitted)

    header = "".join([
        "100", "_" * 13, start_date_formatted, end_date_formatted, "GB", "GBP", "_" * 47
    ])
    header_df = spark.createDataFrame([(header,)], ["EPO Output"])

    footer = "".join([
        "900", "_" * 12, "C", "_" * 2, ipo_total_whole, ipo_total_hundredths,
        epo_total_whole, epo_total_hundredths, date_today_formatted, "POA 35/28", "_" * 17
    ])
    footer_df = spark.createDataFrame([(footer,)], ["EPO Output"])

    return header_df, footer_df

@f.udf(returnType=ArrayType(StringType()))
def generate_epo_renewal_output_udf(row: Row, epo_renewal_pct_split: int):
    """
    Generates the EPO renewal output data.

    Arguments:
        row: Row containing the input data.
        epo_renewal_pct_split: Integer representing the EPO renewal percentage split.

    Returns:
        list: List containing the formatted EPO renewal output strings.
    """
    def create_output_dict(base_dict, renewal_fee, renewal_year, date_filed):
        return {
            **base_dict,
            "space_block": "__",
            "renewal_year": renewal_year,
            "reduced_lor": "00",
            "currency_ind": "_"*2,
            "ipo_amount": "0" * 10,
            "year_amount_whole": renewal_fee[0],
            "year_amount_hundredths": renewal_fee[1],
            "pct_split": "0000",
            "date_filed": date_filed,
            "end_space": "0" * 10 + "____"
        }

    ipo_dict = {
        "record_identifier": "200",
        "publication_prefix": f"_{row.publication_num[:2]}B",
        "publication_number": f"_{row.publication_num[2:]}",
        "payment_indicator": "C",
        "filing_date": row.filing_date,
        "renewal_due_date": row.renewal_due_date,
    }

    output_list = []
    if int(row.renewal_fee_year3[0]) > 0:
        output_list.append(create_output_dict(ipo_dict, row.renewal_fee_year3, "03", row.date_filed))
    if int(row.renewal_fee_year4[0]) > 0:
        output_list.append(create_output_dict(ipo_dict, row.renewal_fee_year4, "04", row.date_filed))

    ipo_dict.update({
        "space_block": "_" * 2,
        "renewal_year": "{:02d}".format(row.renewal_year),
        "reduced_lor": row.lor_endorsement,
        "currency_ind": "_"*2,
        "ipo_amount_whole": row.renewal_fee[0],
        "ipo_amount_hundredths": row.renewal_fee[1],
        "epo_amount_whole": row.renewal_fee_epo[0],
        "epo_amount_hundredths": row.renewal_fee_epo[1],
        "pct_split": str(epo_renewal_pct_split) + "00",
        "date_filed": row.date_filed,
        "end_space": "0"*10 + "_"*4
    })
    output_list.append(ipo_dict)

    return ["".join(item.strip() for item in output.values()) for output in output_list]

@f.udf(returnType=StringType())
def generate_epo_output_udf(row: Row, record_type: int):
    """
    Generates the EPO output string based on the record type and row data.

    Arguments:
        row: Row containing the data for the record.
        record_type: Integer representing the type of record (400, 500, or 600).

    Returns:
        str: Formatted EPO output string.
    """
    if record_type == 400:
        lapse_code = {
            "Ceased": "00",
            "Surrendered": "05",
            "Revoked": "07"
        }.get(row.lapsed_reason, "")

        lapsed_dict = {
            "record_identifier": "400",
            "publication_prefix": f"_{row.publication_num[:2]}B",
            "publication_number": f"_{row.publication_num[2:]}",
            "update_indicator": "_",
            "filing_date": row.filing_date,
            "space_block_1": "_" * 10,
            "lapse_code": lapse_code,
            "space_block_2": "_" * 26,
            "lapse_date": row.date_processed,
            "space_block_3": "_" * 14,
        }
        return "".join(item.strip() for item in lapsed_dict.values())

    elif record_type == 500:
        restored_dict = {
            "record_identifier": "500",
            "publication_prefix": f"_{row.publication_num[:2]}B",
            "publication_number": f"_{row.publication_num[2:]}",
            "update_indicator": "_",
            "filing_date": row.filing_date,
            "space_block_1": "_" * 38,
            "restore_date": row.date_processed,
            "space_block_2": "_" * 14,
        }
        return "".join(item.strip() for item in restored_dict.values())
    
    elif record_type == 600:
        lor_dict = {
            "record_identifier": "600",
            "publication_prefix": f"_{row.publication_num[:2]}B",
            "publication_number": f"_{row.publication_num[2:]}",
            "update_indicator": "_" if row.Addition_Deletion == "A" else "D",
            "filing_date": row.filing_date,
            "lor_date": row.date_processed,
            "first_reduced_year": "{:02d}".format(row.first_reduced_year),
            "end_year": "{:02d}".format(row.last_reduced_year),
            "pct_reduction": "50",
            "space_block": "_" * 46
        }
        return "".join(item.strip() for item in lor_dict.values())

def process_epo_output(data_df, record_type, filter_condition=None, epo_renewal_pct_split=epo_renewal_pct_split) -> pyspark.sql.DataFrame:
    """
    Processes the EPO output data based on the record type and filter condition.

    Arguments:
        data_df: PySpark DataFrame containing the input data.
        record_type: Integer representing the type of record to process.
        filter_condition: String representing the condition to filter the data (default is None).
        epo_renewal_pct_split: Integer representing the EPO renewal percentage split.

    Returns:
        pyspark.sql.DataFrame: PySpark DataFrame containing the processed EPO output data.
    """
    filtered_data_df = data_df.dropna()
    null_df = data_df.subtract(filtered_data_df)
    null_count = null_df.count()
    _logger.info(f"Removed {null_count} type {record_type} records with NULL values from input.")

    if filter_condition is not None:
        filtered_data_df = filtered_data_df.filter(filter_condition)

    filtered_data_df = filtered_data_df \
        .withColumn("filing_date", f.date_format(f.to_date("filing_date"), "ddMMyy"))  \
        .withColumn("date_processed", f.date_format(f.to_date("date_processed"), "ddMMyy")) \
        .withColumn("type", f.lit(record_type))
    
    _logger.info(f"Generating EPO output strings for type {record_type} records.")
    if record_type == 200:
        filtered_data_df = filtered_data_df \
            .withColumn("grant_date", f.date_format(f.to_date("grant_date"), "ddMMyy")) \
            .withColumn("date_filed", f.date_format(f.to_date("date_filed"), "ddMMyy")) \
            .withColumn("renewal_due_date", f.date_format(f.to_date("renewal_due_date"), "ddMMyy")) \
            .withColumn("lor_endorsement", f.when(f.col("lor_endorsement") == "NO", "00").otherwise("02")) 
        
        fee_columns = [col_name for col_name in filtered_data_df.columns if "fee" in col_name]
        for fee_col in fee_columns:
            filtered_data_df = filtered_data_df.withColumn(fee_col, format_fee_data_udf(fee_col))

        output_df = filtered_data_df.withColumn("EPO Output", generate_epo_renewal_output_udf(f.struct([filtered_data_df[x] for x in filtered_data_df.columns]), f.lit(epo_renewal_pct_split)))
        output_df = output_df.withColumn("EPO Output", f.explode(f.col("EPO Output"))).select("publication_num", "type", "EPO Output")
    else:
        output_df = filtered_data_df.withColumn("EPO Output", generate_epo_output_udf(f.struct([filtered_data_df[x] for x in filtered_data_df.columns]), f.lit(record_type))) \
            .select("publication_num", "type", "EPO Output")

    return output_df

#Data Processing

try:
    _logger.info("Retrieving restoration data.")
    legacy_restore_df = get_legacy_restored_dataframe().withColumn("source", f.lit("Legacy"))
    new_restore_df = get_new_restored_dataframe().withColumn("source", f.lit("Dynamics"))

    combined_restore_df = legacy_restore_df.union(new_restore_df).dropDuplicates() \
        .withColumn("type_identifier", f.lit("Restoration"))
    combined_restore_count = combined_restore_df.count()
    _logger.info(f"{combined_restore_count} combined restored rights.")
except Exception as excp:
    _logger.error(f"Error retrieving restoration data: {excp}.")
    notebook_status = False

try:
    _logger.info("Retrieving licence of right data.")
    legacy_lor_df = get_legacy_lor_dataframe().withColumn("source", f.lit("Legacy"))
    new_lor_df = get_new_lor_dataframe().withColumn("source", f.lit("Dynamics"))

    combined_lor_df = legacy_lor_df.union(new_lor_df).dropDuplicates() \
        .withColumn("type_identifier", f.lit("Licence of Right"))
    combined_lor_count = combined_lor_df.count()
    _logger.info(f"{combined_lor_count} combined licence of rights.")
except Exception as excp:
    _logger.error(f"Error retrieving licence of right data: {excp}.")
    notebook_status = False
try:
    new_renewals_df = get_new_renewals_dataframe().withColumn("late_fee", f.lit(None)).withColumn("late_months", f.lit(None))
    new_renewal_count = new_renewals_df.count()
    _logger.info(f"{new_renewal_count} new world renewals.")

    legacy_renewals_df = get_legacy_renewals_dataframe()
    legacy_renewal_count = legacy_renewals_df.count()
    _logger.info(f"{legacy_renewal_count} legacy renewals.")

    _logger.info(f"Deduplicating renewal records present in both systems.")
    non_duplicates = legacy_renewals_df.join(new_renewals_df, legacy_renewals_df.columns, "leftanti") \
        .withColumn("source", f.lit("Legacy"))
    new_renewals_df = new_renewals_df.withColumn("source", f.lit("Dynamics"))
    renewals_df = non_duplicates.unionByName(new_renewals_df).withColumn("type_identifier", f.lit("Renewals"))

    combined_renewal_count = renewals_df.count()
    _logger.info(f"{combined_renewal_count} total renewal records.")
except Exception as excp:
    _logger.error(f"Error processing renewals data: {excp}.")
    notebook_status = False
_logger.info(f"Combining renewal, restoration and licence of right data.")
combined_df = combine_dataframes(renewals_df, combined_restore_df)
cancelled_lor_df = combined_lor_df.filter(f.col("Addition_Deletion") == "D").drop("Addition_Deletion")
combined_df = combine_dataframes(combined_df, cancelled_lor_df)

try:
    _logger.info(f"Saving history to lake database table.")
    spark.sql("CREATE DATABASE IF NOT EXISTS renewals_ldb")
    combined_df.write.format('delta').mode("overwrite").option("path", f"abfss://renewals@{silver_datalake}/dbo/RenewalHistory").saveAsTable("renewals_ldb.RenewalHistory")
except Exception as excp:
  _logger.error(f"Error occurred while writing renewal history table: {excp}.")
  notebook_status = False
_logger.info(f"Determining due date when renewal was paid.")
renewals_df = calculate_renewal_due_dates(renewals_df)
_logger.info(f"Determining renewals with Licence of Right endorsement.")
lor_endoresment_df = combined_lor_df.groupBy("application_num").pivot("Addition_Deletion").agg(f.last("date_processed"))
renewals_df = renewals_df.join(lor_endoresment_df, how="left", on="application_num")
renewals_df = renewals_df.withColumn("renewal_comparison_date", f.least(f.col("renewal_due_date"), f.col("date_processed")))
renewals_df = renewals_df.withColumn("lor_endorsement", f.when(
        (f.col("A") <= f.col("renewal_comparison_date")) & ((f.col("renewal_comparison_date") < f.col("D")) | (f.col("D").isNull())), f.lit("YES")
    ).otherwise(f.lit("NO"))).drop("A", "D")
_logger.info(f"Determining the first and last renewal years endorsed by Licence of Right.")
combined_lor_df = combined_lor_df.withColumn(
    "renewal_year", f.year(f.col("date_processed")) - (f.year(f.col("filing_date"))-1)
)
combined_lor_df = calculate_renewal_due_dates(combined_lor_df)
combined_lor_df = combined_lor_df.join(
    renewals_df.select(f.col("application_num"), f.col("renewal_year"), f.col("date_processed").alias("renewal_date_processed")),
    how="left", on=["application_num", "renewal_year"]
).drop_duplicates()
combined_lor_df = combined_lor_df.withColumn("renewal_comparison_date", f.least(f.col("renewal_due_date"), f.col("renewal_date_processed")))
combined_lor_df = combined_lor_df.withColumn(
    "first_reduced_year",
    f.when(
        f.col("date_processed") <= f.col("renewal_comparison_date"),
        f.col("renewal_year")
    ).otherwise(f.col("renewal_year")+1)
)

addition_lor_df = combined_lor_df.filter(
    f.col("Addition_Deletion") == "A"
).withColumn(
    "last_reduced_year", f.lit(20)
)

deletion_lor_df = combined_lor_df.filter(
    f.col("Addition_Deletion") == "D"
).drop("first_reduced_year").join(
    addition_lor_df.select(
        f.col("application_num"), f.col("first_reduced_year")
    ),
    how="left", on="application_num"
).dropDuplicates()

deletion_lor_df = deletion_lor_df.withColumn(
    "last_reduced_year",
    f.when(
        f.col("date_processed") <= f.col("renewal_comparison_date"),
        f.col("renewal_year") - 1
    ).otherwise(f.col("renewal_year"))
)

combined_lor_df = addition_lor_df.union(deletion_lor_df) \
    .drop("renewal_year", "renewal_due_date", "renewal_date_processed", "renewal_comparison_date")

combined_lor_df = combined_lor_df.filter(
    f.col("first_reduced_year") <= 20
)
try:
    _logger.info(f"Filtering EP(UK) records between {start_date} and {end_date}.")

    filtered_restore_df = combined_restore_df.filter(
        (f.col("date_processed").between(start_date, end_date)) &
        (f.col("publication_num").like("EP%"))
    )
    filtered_restore_count = filtered_restore_df.count()
    _logger.info(f"{filtered_restore_count} restored rights.")

    filtered_lor_df = combined_lor_df.filter(
        (f.col("date_processed").between(start_date, end_date)) &
        (f.col("publication_num").like("EP%"))
    )
    filtered_lor_count = filtered_lor_df.count()
    _logger.info(f"{filtered_lor_count} licence of rights.")

    # BUSINESS LOGIC: Filter renewals by date_processed (action/completion date) instead of date_filed
    # This ensures renewals are reported in the month they were actually processed/completed,
    # aligning with when the IPO actually receives the fees. This is critical for accurate
    # financial reconciliation with EPO, especially when form filing date and action completion
    # date fall in different months/quarters.
    
    # Log renewals where date_filed and date_processed cross month boundaries (for audit purposes)
    cross_month_renewals = renewals_df.filter(
        (f.col("publication_num").like("EP%")) &
        (f.month(f.col("date_filed")) != f.month(f.col("date_processed")))
    )
    cross_month_count = cross_month_renewals.count()
    if cross_month_count > 0:
        _logger.info(f"{cross_month_count} renewals have date_filed and date_processed in different months.")
    
    filtered_renewals_df = renewals_df.filter( 
        (f.col("date_processed").between(start_date, end_date)) &
        (f.col("publication_num").like("EP%"))
    )
    filtered_renewals_count = filtered_renewals_df.count()
    _logger.info(f"{filtered_renewals_count} renewals.")
except Exception as excp:
    _logger.error(f"Unable to filter inputs according to specified date ranges: {excp}.")
    notebook_status = False

try:
    legacy_lapse_df = get_legacy_lapsed_dataframe().withColumn("source", f.lit("Legacy"))
    new_lapse_df = get_new_lapsed_dataframe().withColumn("source", f.lit("Dynamics"))

    combined_lapsed_df = legacy_lapse_df.union(new_lapse_df).dropDuplicates()
    combined_lapsed_count = combined_lapsed_df.count()
    _logger.info(f"{combined_lapsed_count} lapsed rights.")
except Exception as excp:
    _logger.error(f"Error retrieving lapsed data: {excp}.")
    notebook_status = False
_logger.info("Calculating fees owed to EPO, including any share of year 3 and year 4 renewals.")
filtered_renewals_df = (
    calculate_renewal_fee_splits(
        filtered_renewals_df, fee_amounts_df, epo_renewal_pct_split, interest_due_year3, interest_due_year4
    )
    .drop("late_fee", "late_months")
)

#Output Generation

_logger.info(f"Generating fee totals for period between {start_date} and {end_date}.")
grand_total_prescribed = filtered_renewals_df.select(f.sum("renewal_fee")).collect()[0][0]
grand_total_remitted = filtered_renewals_df.select(f.sum("total_epo_amount")).collect()[0][0]
grand_total_retained = grand_total_prescribed - grand_total_remitted

totals = {
    "Grand Total Prescribed UK Fees": grand_total_prescribed,
    "Grand Total to be Remitted to EPO": grand_total_remitted,
    "Grand Total Retained": grand_total_retained
}

#Generate Monthly EP(UK) Renewal Internal Report

if not quarterly:
    _logger.info(f"Generating monthly EP(UK) renewal internal report.")
    cols = [
        "publication_num",
        "renewal_due_date",
        "renewal_year",
        "lor_endorsement",
        "renewal_fee",
        "renewal_fee_year3",
        "renewal_fee_year4",
        "total_epo_amount",
        "source"
    ]
    monthly_internal_df = filtered_renewals_df.select(*cols).orderBy(f.col("renewal_due_date"))

    monthly_internal_df = monthly_internal_df.withColumnsRenamed({
        "publication_num": "European Patent (UK) Publication Number",
        "renewal_due_date": "Due Date For Payment Of Renewal",
        "renewal_year": "Ordinal Number (Annuity)",
        "lor_endorsement": "Licence Of Right Endorsement",
        "renewal_fee": "Prescribed UK Fee",
        "renewal_fee_epo": "Proportion of UK Prescribed Fee to be Remitted to EPO",
        "renewal_fee_year3": "3rd Year EPO Minimum Amount",
        "renewal_fee_year4": "4th Year EPO Minimum Amount",
        "total_epo_amount": "Total to be Remitted to EPO",
    })

    monthly_totals_df = spark.createDataFrame(list(totals.items()), ["Description", "Amount"]) \
        .withColumn("Amount", f.format_number("Amount", 2))

    try:
        _logger.info(f"Saving monthly renewals report to datalake.")
        monthly_internal_df.orderBy("Due Date For Payment Of Renewal").coalesce(1).write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(f"abfss://renewals@{silver_datalake}/ipo/{date_today.strftime('%d%m%y')}/ipomonthlyreport.csv")
        
        monthly_totals_df.coalesce(1).write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(f"abfss://renewals@{silver_datalake}/ipo/{date_today.strftime('%d%m%y')}/ipomonthlytotals.csv")
    except Exception as excp:
        _logger.error(f"Error occurred while generating monthly renewals report: {excp}.")
        notebook_status = False

#Generate Quarterly EP(UK) Status Internal Report

if quarterly:
    _logger.info(f"Generating quarterly EP(UK) status internal report.")
    lapsed_status_df = combined_lapsed_df.withColumn("type", f.col("lapsed_reason")).select(
        "publication_num", "filing_date", "date_processed", "type"
    )
    restore_status_df = filtered_restore_df.select(
        f.col("publication_num"), f.col("filing_date"), f.col("date_processed"), f.col("type_identifier").alias("type")
    )
    lor_status_df = filtered_lor_df.select(
        f.col("publication_num"), 
        f.col("filing_date"), 
        f.col("Addition_Deletion"),
        f.col("first_reduced_year").alias("from_year"),
        f.col("last_reduced_year").alias("to_year"),
        f.col("date_processed"),
        f.col("type_identifier").alias("type")
    )

    combined_status_df = lapsed_status_df \
        .unionByName(restore_status_df, allowMissingColumns=True) \
        .unionByName(lor_status_df, allowMissingColumns=True)

    # BUSINESS LOGIC: Group monthly totals by date_processed (action/completion date)
    # This ensures renewals are reported in the month they were actually processed/completed,
    # matching the filtering logic above and aligning with when fees are received by IPO.
    epo_monthly_totals = filtered_renewals_df.groupBy(f.date_format("date_processed", "MMMM yyyy").alias("Month"), 
            f.date_format("date_processed", "MM").cast("int").alias("Month_Number")) \
        .agg(f.sum("total_epo_amount").alias("Amount")) \
        .orderBy(f.col("Month_Number")) \
        .drop("Month_Number")

    qrtly_totals_df = filtered_renewals_df.agg(f.sum("total_epo_amount").alias("Amount")) \
        .withColumn("Month", f.lit("EPO Total"))
    qrtly_totals_df = epo_monthly_totals.unionByName(qrtly_totals_df) \
        .withColumn("Amount", f.format_number("Amount", 2))

    try:
        _logger.info(f"Saving quarterly status report to datalake.")
        combined_status_df.orderBy("type", "filing_date").coalesce(1).write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(f"abfss://renewals@{silver_datalake}/ipo/{date_today.strftime('%d%m%y')}/ipoqrtrlyreport.csv")

        qrtly_totals_df.coalesce(1).write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(f"abfss://renewals@{silver_datalake}/ipo/{date_today.strftime('%d%m%y')}/ipoqrtrlytotals.csv")
    except Exception as excp:
        _logger.error(f"Error occurred while generating quarterly status report: {excp}.")
        notebook_status = False

#Generate Quarterly EPO Reconciliation File

if quarterly:
    try:
        qrtly_renewals_df = process_epo_output(filtered_renewals_df, 200)
        qrtly_lapsed_df = process_epo_output(combined_lapsed_df, 400, f.col("lapsed_reason") != "Expired")
        qrtly_restore_df = process_epo_output(filtered_restore_df, 500)
        qrtly_lor_df = process_epo_output(filtered_lor_df, 600)

        combined_qrtly_df = qrtly_renewals_df.union(qrtly_lapsed_df).union(qrtly_restore_df).union(qrtly_lor_df)
        combined_qrtly_df = combined_qrtly_df.orderBy("type", "publication_num")

        header_df, footer_df = generate_header_footer_information()
        epo_df = header_df.union(combined_qrtly_df.select("EPO Output")).union(footer_df)
        
        epo_filtered_df = epo_df.filter(f.length(f.col("EPO Output")) == 80)
        epo_filtered_df = epo_filtered_df.withColumn("EPO Output", f.regexp_replace(f.col("EPO Output"), "_", " "))
        epo_filtered_df = epo_filtered_df.drop_duplicates()

        reject_df = epo_df.filter(f.length(f.col("EPO Output")) != 80)
        reject_count = reject_df.count()
        if reject_count > 0:
            _logger.info(f"{reject_count} records with invalid EPO Output.")
            notebook_status = False
    except Exception as excp:
         _logger.error(f"Unable to generate EPO output: {excp}.")
         notebook_status = False
if quarterly:
    try:
        _logger.info("Writing EPO Reconciliation file to data lake.")
        epo_filtered_df.orderBy("EPO Output").coalesce(1).write.format("text") \
            .option("header", "false") \
            .option("encoding", "ISO-8859-1") \
            .mode("overwrite") \
            .save(f"abfss://renewals@{silver_datalake}/epo/{date_today.strftime('%d%m%y')}/epoqrtrly{date_today.strftime('%d%m%y')}.txt")
    except Exception as excp:
        _logger.error(f"Error occurred while writing EPO reconciliation file: {excp}.")
        notebook_status = False
mssparkutils.notebook.exit(notebook_status)