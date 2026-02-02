import os
import json
import time
import logging
import requests
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import Agency, Agency_Contact, Customer, Source_System
from df_database_models.db_utils import generate_uuid, convert_timestamps, query_update_dict, get_record, multi_filter_get_record, push_msg_to_sqs, call_sp
from secrets_manager import get_secret
from datetime import datetime
import pandas as pd
import asyncio
from adf_pyutils.clm_wrapper import common_logger

# os.environ["AWS_REGION"] = "us-east-1"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# # ... Set up all environment variables as before

# --- Global DB config ---
pnc_db = os.environ['RDS_DB_NAME']
ref_db = os.environ['RDS_REF_DB_NAME']
mdm_raw_db = os.environ['RDS_RAW_DB_NAME']
mdm_refined_db = os.environ['RDS_REFINED_DB_NAME']

async def log_msg(func,**kwargs):
    await asyncio.to_thread(func,**kwargs)

# --- Session/Engine Initializer ---
def call_session_engine(source_system=None, database_name=None):

    rds_secret_name = os.environ["RDS_SECRETS_MANAGER_ID"]
    region_name = os.environ["AWS_REGION"]
    rds_host_nm = os.environ['RDS_HOST']

    if database_name == 'ref_data':
        rds_db_nm = os.environ['RDS_REF_DB_NAME']
    elif database_name == 'mdm_raw':
        rds_db_nm = os.environ['RDS_RAW_DB_NAME']
    elif database_name == 'mdm_refined':
        rds_db_nm = os.environ['RDS_REFINED_DB_NAME']
    else:
        rds_db_nm = os.environ['RDS_DB_NAME']

    if source_system and source_system.lower() == 'as400_affprd':
        as400_secret_name = os.environ["AS400_AFF_SECRETS_MANAGER_ID"]
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    elif source_system and source_system.lower() == 'as400_kkins':
        as400_secret_name = os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    elif source_system and source_system.lower() == 'as400_attorney':
        as400_secret_name = os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    if database_name:
        session = get_rds_db_session(rds_secret_name, region_name, rds_host_nm, rds_db_nm)
        return session



session = call_session_engine(database_name=pnc_db)
as400_engine_aff = call_session_engine(source_system='as400_affprd')
as400_engine_kkins = call_session_engine(source_system='as400_kkins')
as400_engine_attorney = call_session_engine(source_system='as400_attorney')

def lookup_as400(config=None, id=None):

    source_system = config['source_system']
    if source_system:
        if(source_system.lower() == 'as400_affprd'):
                df = pd.read_sql(f"""
                        
                SELECT 
                    CAST(NULL AS varchar(255)) AS df_agency_contact_id,
                    CAST(NULL AS varchar(255)) AS raw_agency_contact_id,
                    --CAST(NULL AS varchar(255)) AS source_agency_contact_id,
                    CAST(NULL AS varchar(255)) AS df_agency_id,
                    LTRIM(RTRIM(SUBPNO)) AS source_agency_id,
                    LTRIM(RTRIM(SUBFNM)) AS first_name,
                    LTRIM(RTRIM(SUBMNM)) AS middle_name,
                    LTRIM(RTRIM(SUBLNM)) AS last_name,
                    (LTRIM(RTRIM(SUBLNM)) + ', ' + LTRIM(RTRIM(SUBFNM)) + ' ' + LTRIM(RTRIM(SUBMNM))) as full_name,
                    CAST(NULL AS varchar(255)) AS source_contact_type,
                    CASE 
                        WHEN LOWER(LTRIM(RTRIM(SUACTI))) = 'n' THEN 'Terminated'
                        WHEN LOWER(LTRIM(RTRIM(SUACTI))) = 'y' THEN 'Active'
                        ELSE NULL
                    END AS agency_contact_status,
                    LTRIM(RTRIM(SUEMAL)) AS email,
                    CAST(NULL AS varchar(255)) AS ssn,
                    CAST(NULL AS varchar(255)) AS passport_number,
                    LTRIM(RTRIM(SUTXID)) AS tax_number,
                    LTRIM(RTRIM(SUBAD1)) AS premises_street_line1,
                    LTRIM(RTRIM(SUBAD2)) AS premises_street_line2,
                    LTRIM(RTRIM(SUBCTY)) AS premises_city,
                    CAST(NULL AS varchar(255)) AS premises_county,
                    LTRIM(RTRIM(SUBSTC)) AS premises_state,
                    CAST(NULL AS varchar(255)) AS premises_country,
                    LTRIM(RTRIM(SUBZIP)) AS premises_zip_code,
                    CAST(NULL AS varchar(255)) AS premises_latitude,
                    CAST(NULL AS varchar(255)) AS premises_logitude,
                    LTRIM(RTRIM(SUBAD1)) AS mailing_address_line1,
                    LTRIM(RTRIM(SUBAD2)) AS mailing_address_line2,
                    LTRIM(RTRIM(SUBCTY)) AS mail_city,
                    CAST(NULL AS varchar(255)) AS mail_county,
                    LTRIM(RTRIM(SUBSTC)) AS mail_state,
                    CAST(NULL AS varchar(255)) AS mailing_country,
                    LTRIM(RTRIM(SUBZIP)) AS mailing_zip,
                    CAST(NULL AS varchar(255)) AS mailing_latitude,
                    CAST(NULL AS varchar(255)) AS mailing_longitude,
                    LTRIM(RTRIM(SUBAD1)) AS billing_address_line1,
                    LTRIM(RTRIM(SUBAD2)) AS billing_address_line2,
                    LTRIM(RTRIM(SUBCTY)) AS billing_city,
                    CAST(NULL AS varchar(255)) AS billing_county,
                    LTRIM(RTRIM(SUBSTC)) AS billing_state,
                    CAST(NULL AS varchar(255)) AS billing_country,
                    LTRIM(RTRIM(SUBZIP)) AS billing_zip,
                    CAST(NULL AS varchar(255)) AS billing_latitude,
                    CAST(NULL AS varchar(255)) AS billing_longitude,
                    LTRIM(RTRIM([SUPHN#])) AS home_phone,
                    LTRIM(RTRIM([SUPHN#])) AS work_phone,
                    LTRIM(RTRIM([SUPHN#])) AS mobile_phone,
                    --LTRIM(RTRIM([SUFAX#])) AS invoice_fax,
                    LTRIM(RTRIM([SUFAX#])) AS fax, --premises_fax
                    --LTRIM(RTRIM([SUFAX#])) AS mail_fax
                    LTRIM(RTRIM(SUWEBA)) AS website,
                    CAST(NULL AS varchar(255)) AS created_date,
                    CAST(NULL AS varchar(255)) AS modified_date,
                    'AS400_AFFPRD' AS source_system,
                    CAST(NULL AS varchar(255)) AS df_source_system_id,
                    CAST(NULL AS varchar(255)) AS mdm_agency_contact_id
                FROM adgdtadv.adgsubp
                where LTRIM(RTRIM(SUBPNO)) = '{id}';
                    """, con=as400_engine_aff)
        elif(source_system.lower() == 'as400_attorney'):
                    df = pd.read_sql(f"""
                                    SELECT CAST(NULL AS varchar(255)) AS df_agency_contact_id
                                    ,CAST(NULL AS varchar(255)) AS raw_agency_contact_id
                                    ,CAST(AAGNCODE AS varchar(255)) AS source_agency_contact_id
                                    ,AEMAILAD AS email
                                    ,AEMAILAD AS email_operational
                                    ,AADDRES1 AS premises_address_line_1
                                    ,AADDRES2 AS premises_address_line_2
                                    ,ACITY AS premises_city
                                    --,CAST(NULL AS varchar(255)) AS premises_county
                                    ,ASTATE AS premises_state
                                    --,CAST(NULL AS varchar(255)) AS premises_country
                                    ,CAST(AZIPCODE AS varchar(255)) AS premises_zip
                                    ,CAST(APHONENU AS varchar(255)) AS premises_phone
                                    ,AADDRES1 AS mailing_address_line_1
                                    ,AADDRES2 AS mailing_address_line_2
                                    ,ACITY AS mailing_city
                                    --,CAST(NULL AS varchar(255)) AS mailing_county
                                    ,ASTATE AS mailing_state
                                    --,CAST(NULL AS varchar(255)) AS mailing_country
                                    ,CAST(AZIPCODE AS varchar(255)) AS mailing_zip
                                    ,CAST(APHONENU AS varchar(255)) AS mailing_phone
                                    ,AADDRES1 AS billing_address_line_1
                                    ,AADDRES2 AS billing_address_line_2
                                    ,ACITY AS billing_city
                                    --,CAST(NULL AS varchar(255)) AS billing_county
                                    ,ASTATE AS billing_state
                                    --,CAST(NULL AS varchar(255)) AS billing_country
                                    ,CAST(AZIPCODE AS varchar(255)) AS billing_zip
                                    ,CAST(APHONENU AS varchar(255)) AS billing_phone
                                    ,'AS400_Attorney' AS source_system
                                    FROM LAWDTAPR.LAWAGNCP 
                                    where AAGNCODE = {id}
                                    """, con=as400_engine_attorney)
        elif(source_system.lower() == 'as400_kkins'):
                df = pd.read_sql(f"""
                        SELECT DISTINCT 
                        (CAST(agnt.AGT_OFF_CODE as varchar(255)) + '-' + CAST(agnt.AGY_AGENT_NBR as varchar(255)) + '-' + CAST(agnt.AGY_AGENT_CODE as varchar(255))) AS source_agency_contact_id,
                        (CAST(agcy.AGY_OFFICE as varchar(255)) + '-' + CAST(agcy.AGY_NUMBER as varchar(255))) AS source_agency_id,
                        'AS400_KKINS' AS source_system,
                        CAST(NULL AS varchar(36)) AS mdm_agency_contact_id
                    FROM [PLCYPROD].[APZ001] AS agcy
                    INNER JOIN [PLCYPROD].[APZ002] AS agnt ON agcy.AGY_OFFICE = agnt.AGT_OFF_CODE AND agcy.AGY_NUMBER = agnt.AGY_AGENT_NBR
                    INNER JOIN [PLCYPROD].[APZ001TG] AS gt ON agcy.AGY_OFFICE = gt.AGY_OFFICE AND agcy.AGY_NUMBER = gt.AGY_NUMBER
                    INNER JOIN [PLCYPROD].[AUZ002] AS pl ON agnt.AGY_AGENT_NBR = pl.AGY_AGENT_NBR AND agnt.AGY_AGENT_CODE = pl.AGY_AGENT_CODE
                    WHERE (CAST(agcy.AGY_OFFICE as varchar(255)) + '-' + CAST(agcy.AGY_NUMBER as varchar(255))) = '{id}';
                    """, con=as400_engine_kkins)
    else:
        df=None

    if(len(df)>0):
        return df.to_dict('records')[0]
    else:
        return None


def consume_lambda(config=None):
    """
    Ingestion script for Agency_Contact table.
    Matches your existing pattern for other entities.
    Does NOT insert into any master table. Only inserts/updates agency_contact.
    """

    now = datetime.now()
    start_ts = datetime.timestamp(now)

    logger.info(f"Processing AS400 Agency Contact @ {now}")

    try:
        # Normalize input config
        config_list = (
            config if isinstance(config, list)
            else [json.loads(config)] if isinstance(config, str)
            else [config]
        )

        for cfg in config_list:

            src_agency_contact_id = cfg.get("source_agency_id")
            source_system = cfg.get("source_system", "").lower()

            if not src_agency_contact_id:
                logger.info(f"Missing source_agency_id")
                continue

            logger.info(f"Invoking handler for agency_contact {src_agency_contact_id} from {source_system}"
                )

            # ---------------------------------------------------------------
            # 1. FETCH FROM AS400
            # ---------------------------------------------------------------

            as400_contact_dict = lookup_as400(
                config=cfg,
                id=src_agency_contact_id
            )

            if not as400_contact_dict:
                logger.info(f"No AS400 agency_contact record found for {src_agency_contact_id}")
                continue

            logger.info(f"AS400 Agency Contact Data Fetched"
                )

            # ---------------------------------------------------------------
            # 2. FK Lookup: Source System (NO insert)
            # ---------------------------------------------------------------

            ss_rec = session.query(Source_System).filter(
                Source_System.source_system == as400_contact_dict.get("source_system")).first()
            if not ss_rec:
                logger.info(f"Source system not found — skipping agency_contact {src_agency_contact_id}"
                    )
                continue

            df_source_system_id = ss_rec.df_source_system_id
            as400_contact_dict["df_source_system_id"] = df_source_system_id

            # ---------------------------------------------------------------
            # 3. FK Lookup: Agency contact using LIKE
            # ---------------------------------------------------------------

            src_agency_id = as400_contact_dict.get("source_agency_id")

            agency_rec = session.query(Agency).filter(
                Agency.source_agency_id == src_agency_id,
                Agency.df_source_system_id == df_source_system_id
            ).first()


            if not agency_rec:
                logger.info(f"Agency NOT FOUND for source_agency_id {src_agency_id}; skipping agency_contact"
                    )
                continue

            as400_contact_dict["df_agency_id"] = agency_rec.df_agency_id

            # ---------------------------------------------------------------
            # 4. EXISTING AGENCY_CONTACT CHECK
            # ---------------------------------------------------------------

            existing_contact = session.query(Agency_Contact).filter(
                Agency_Contact.df_agency_id == agency_rec.df_agency_id,
                Agency_Contact.df_source_system_id == df_source_system_id
            ).first()

            src_agency_id = agency_rec.df_agency_id

            # ---------------------------------------------------------------
            # 5. INSERT LOGIC
            # ---------------------------------------------------------------

            if not existing_contact:

                df_agency_contact_id = generate_uuid(
                    str(src_agency_id),
                    str(df_source_system_id)
                )

                as400_contact_dict["df_agency_contact_id"] = df_agency_contact_id

                # IMPORTANT: created_at & modified_at for INSERT
                as400_contact_dict["created_at"] = (
                    convert_timestamps(as400_contact_dict.get("created_date"))
                    if as400_contact_dict.get("created_date")
                    else datetime.now()
                )

                as400_contact_dict["modified_at"] = (
                    convert_timestamps(as400_contact_dict.get("modified_date"))
                    if as400_contact_dict.get("modified_date")
                    else datetime.now()
                )

                session.add(
                    Agency_Contact.from_dict(cls=Agency_Contact, d=as400_contact_dict)
                )
                session.commit()

                logger.info(f"Inserted Agency Contact {src_agency_contact_id}"
                    )

            # ---------------------------------------------------------------
            # 6. UPDATE LOGIC
            # ---------------------------------------------------------------

            else:

                as400_contact_dict["df_agency_contact_id"] = existing_contact.df_agency_contact_id

                # Only update modified_at
                as400_contact_dict["modified_at"] = datetime.now()

                # NEVER update created_at
                if "created_at" in as400_contact_dict:
                    del as400_contact_dict["created_at"]

                q = session.query(Agency_Contact).filter(
                    Agency_Contact.df_agency_contact_id == existing_contact.df_agency_contact_id
                )

                q.update(query_update_dict(obj=Agency_Contact, dict=as400_contact_dict))
                session.commit()

                logger.info(f"Updated Agency Contact {src_agency_contact_id}"
                    )

        end_ts = datetime.timestamp(datetime.now())
        return {"execution_time_sec": end_ts - start_ts}

    except SQLAlchemyError as e:
        session.rollback()
        logger.info(f"DB Error")
        raise e


def handle(event, context):
    start_time = time.time()
    failures = []

    for record in event.get("Records", []):
        logger.info(record)
        payload = record.get("body")
        try:

            consume_lambda(config=payload)   # sync call is correct
        except Exception:
            message_id = record.get("messageId")
            if message_id:
                failures.append({"itemIdentifier": message_id})
            else:
                logger.error("Record missing messageId: %s", record, exc_info=True)

    end_time = time.time()
    logger.info("Lambda handle duration: %.3f seconds", end_time - start_time)

    return {
        "batchItemFailures": failures
    }


# if __name__ == '__main__':
#     print("Script entrypoint reached.")
#     result = handle({'Records': [{'body': '{"source_agency_id":"FW-3261", "source_system":"AS400_KKINS"}'}]}, None)
#     print("Handle executed.")
#     print(f"Result: {result}")







Customer




import os
import json
import logging
import time
from sqlalchemy.exc import SQLAlchemyError
from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import Customer, Source_System
from df_database_models.db_utils import (
    generate_uuid,
    convert_timestamps,
    query_update_dict,
    get_record,
    multi_filter_get_record
)
from secrets_manager import get_secret
from datetime import datetime
import pandas as pd
import asyncio
from adf_pyutils.clm_wrapper import common_logger

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# ---------------- Environment Variables ---------------- #

pnc_db = os.environ['RDS_DB_NAME']
ref_db = os.environ['RDS_REF_DB_NAME']
mdm_raw_db = os.environ['RDS_RAW_DB_NAME']
mdm_refined_db = os.environ['RDS_REFINED_DB_NAME']


# ---------------- Utility Functions ---------------- #
async def log_msg(func, **kwargs):
    await asyncio.to_thread(func, **kwargs)

def call_session_engine(source_system=None, database_name=None):
    """Get DB sessions (RDS / AS400)"""

    rds_secret_name = os.environ["RDS_SECRETS_MANAGER_ID"]
    region_name = os.environ["AWS_REGION"]
    rds_host_nm = os.environ['RDS_HOST']

    if database_name == 'ref_data':
        rds_db_nm = os.environ['RDS_REF_DB_NAME']
    elif database_name == 'mdm_raw':
        rds_db_nm = os.environ['RDS_RAW_DB_NAME']
    elif database_name == 'mdm_refined':
        rds_db_nm = os.environ['RDS_REFINED_DB_NAME']
    else:
        rds_db_nm = os.environ['RDS_DB_NAME']

    if source_system and source_system == 'as400_affprd':
        as400_secret_name = os.environ["AS400_AFF_SECRETS_MANAGER_ID"]
        return get_as400_db_session(as400_secret_name, region_name)

    elif source_system and source_system == 'as400_kkins':
        as400_secret_name = os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        return get_as400_db_session(as400_secret_name, region_name)

    elif source_system and source_system == 'as400_attorney':
        as400_secret_name = os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        return get_as400_db_session(as400_secret_name, region_name)

    # Default: RDS session
    return get_rds_db_session(rds_secret_name, region_name, rds_host_nm, rds_db_nm)


# ---------------- Global Sessions ---------------- #
session = call_session_engine(database_name=pnc_db)
as400_engine_aff = call_session_engine(source_system='as400_affprd')
as400_engine_kkins = call_session_engine(source_system='as400_kkins')

# ---------------- Lookup ---------------- #
def lookup_as400(config, id):
    """Lookup customer data from AS400"""
    source_system = config['source_system']
    df = None
    if source_system:
        if source_system.lower() == 'as400_affprd':
            df = pd.read_sql(f"""
            SELECT DISTINCT
                CAST(NULL AS varchar(36)) AS df_customer_id,
                cu.CUSKEY AS source_customer_id,
                --CAST(NULL AS varchar(36)) AS df_source_system_id,
                '2' AS df_source_system_id,
                CAST(NULL AS varchar(36)) AS mdm_customer_id,
                NULL AS created_at,         
                NULL AS modified_at
            FROM ADGDTADV.NSOCVGP AS pol
            INNER JOIN ADGDTADV.HCPCSTP AS cust ON pol.customer_no = cust.CUSTNO
            INNER JOIN ADGDTADV.HCPCUSP AS cu ON cust.CSTKEY = cu.CUSKEY
            Where cu.CUSKEY = {id}
            """, con=as400_engine_aff)

        elif source_system.lower() == 'as400_kkins':
            df = pd.read_sql(f"""
            SELECT DISTINCT 
                CAST(null as varchar(255)) as df_customer_id,
                an.AC_ACCOUNT_NBR AS source_customer_id, 
                'AS400_KKINS' as source_system,
                CAST(null as varchar(255)) as mdm_customer_id,
                CAST(null as timestamp) as created_at,
                CAST(null as timestamp) as modified_at
            FROM PLCYPROD.ANZ002 an
            INNER JOIN USERLIB.PLCYDATA pl 
                ON an.AC_ACCOUNT_NBR = pl.AC_ACCOUNT_NBR
            WHERE an.AC_ACCOUNT_NBR = '{id}';
            """, con=as400_engine_kkins)

        if df is not None and len(df) > 0:
            return df.to_dict('records')
        return None


# ---------------- Main Consumer ---------------- #
def consume_lambda(config=None):
    logger.info('consume lambda invoked')
    now = datetime.now()
    start_timestamp = datetime.timestamp(now)

    try:
        config_dicts = config if isinstance(config, dict) else json.loads(str(config))

        if not isinstance(config_dicts, list):
            config_dicts = [config_dicts]

        for config_dict in config_dicts:

            customer_id = config_dict['source_customer_id']
            source_system = config_dict['source_system']

            if not customer_id:
                continue

            as400_customer_summary_dicts = lookup_as400(config_dict, customer_id)

            # if 
            for as400_customer_summary_dict in as400_customer_summary_dicts:
                if as400_customer_summary_dict:
                    logger.info( f'Initial {source_system}')

                    # Customer record check
                    source_customer_id = as400_customer_summary_dict["source_customer_id"]
                    
                    # -----------------------------------------------------------------
                    # 2. FK LOOKUP — source_system (DO NOT INSERT)
                    # -----------------------------------------------------------------

                    ss_rec = session.query(Source_System).filter(
                        Source_System.source_system == as400_customer_summary_dict.get("source_system")
                    ).first()

                    if ss_rec:
                        as400_customer_summary_dict["df_source_system_id"] = ss_rec.df_source_system_id
                    else:
                        logger.info(f"Source system not found — skipping customer {source_customer_id}")
                        continue

                    df_source_system_id = as400_customer_summary_dict["df_source_system_id"]

                    customer_record = multi_filter_get_record(
                        session, model=Customer,
                        source_customer_id=source_customer_id,
                        source_system_id=df_source_system_id
                    )
                    self_customer = customer_record.first() if customer_record is not None else None

                    if self_customer is None:
                        df_customer_id = generate_uuid(
                            str(source_customer_id)
                        )

                        as400_customer_summary_dict['df_customer_id'] = df_customer_id
                        existing_uuid = session.query(Customer).filter(
                            Customer.df_customer_id == df_customer_id
                        ).first()
                        
                        if not existing_uuid :
                            # IMPORTANT: created_at & modified_at for INSERT
                            as400_customer_summary_dict["created_at"] = (
                                convert_timestamps(as400_customer_summary_dict["created_at"])
                                if as400_customer_summary_dict["created_at"]
                                else datetime.now()
                            )
            
                            as400_customer_summary_dict["modified_at"] = (
                                convert_timestamps(as400_customer_summary_dict["modified_at"])
                                if as400_customer_summary_dict["modified_at"]
                                else datetime.now()
                            )
                        
                            session.add(Customer.from_dict(cls=Customer, d=as400_customer_summary_dict))
                            session.commit()
                            logger.info(f'Inserted Customer {customer_id}')
                        else:
                            logger.info(f" Customer already exist with df_customer_id {existing_uuid.df_customer_id}")
                            as400_customer_summary_dict['df_customer_id'] = existing_uuid.df_customer_id
                            # Only update modified_at
                            as400_customer_summary_dict["modified_at"] = datetime.now()

                            q = session.query(Customer).filter(
                                Customer.df_customer_id == existing_uuid.df_customer_id
                            )
                            q.update(query_update_dict(obj=Customer, dict=as400_customer_summary_dict))
                            session.commit()
                            logger.info(f'Updated Customer {customer_id}')

                    else:
                        as400_customer_summary_dict['df_customer_id'] = self_customer.df_customer_id
                        # Only update modified_at
                        as400_customer_summary_dict["created_at"] = self_customer.created_at
                        as400_customer_summary_dict["modified_at"] = datetime.now()

                        q = session.query(Customer).filter(
                            Customer.df_customer_id == self_customer.df_customer_id
                        )
                        q.update(query_update_dict(obj=Customer, dict=as400_customer_summary_dict))
                        session.commit()
                        logger.info(f'Updated Customer {customer_id}')
                else:
                    error_log = {"customer_id": customer_id, "error_message": "No record found after lookup"}
                    logger.info('Lookup failed')

        end_timestamp = datetime.timestamp(datetime.now())
        logger.info(f'Execution Time: {end_timestamp - start_timestamp}s')

    except SQLAlchemyError as e:
        session.rollback()
        logger.info('DB Error')
        raise e

# ---------------- Lambda Handler ---------------- #
def handle(event, context):
    start_time = time.time()
    failures = []

    for record in event.get("Records", []):
        logger.info(record)
        payload = record.get("body")
        try:
            consume_lambda(config=payload)   # sync call is correct
        except Exception:
            message_id = record.get("messageId")
            if message_id:
                failures.append({"itemIdentifier": message_id})
            else:
                logger.error("Record missing messageId: %s", record, exc_info=True)

    end_time = time.time()
    logger.info("Lambda handle duration: %.3f seconds", end_time - start_time)

    return {
        "batchItemFailures": failures
    }



# if __name__ == '__main__':
#    handle({"Records": [{"body": '{ "source_customer_id": "313389", "source_system": "AS400_KKINS"}'}]}, None)
# 




Customer contact


import os
import json
import time
import pandas as pd
import asyncio
import logging
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, and_,select, BIGINT, create_engine, Column, update, Integer, cast, String, MetaData, DateTime, Float, Text, DECIMAL, TIMESTAMP, text
from sqlalchemy.orm import Session, declarative_base
from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import (
    Customer_Contact, Customer, Source_System
)
from df_database_models.db_utils import (
    generate_uuid, convert_timestamps, query_update_dict, get_record, multi_filter_get_record
)
from secrets_manager import get_secret
from adf_pyutils.clm_wrapper import common_logger


logger = logging.getLogger()
logger.setLevel(logging.INFO)
# # ... Set up all environment variables as before

pnc_db = os.environ['RDS_DB_NAME']
ref_db = os.environ['RDS_REF_DB_NAME']
mdm_raw_db = os.environ['RDS_RAW_DB_NAME']
mdm_refined_db = os.environ['RDS_REFINED_DB_NAME']

# --- Async Logger Wrapper ---
async def log_msg(func, **kwargs):
    await asyncio.to_thread(func, **kwargs)

# --- Session/Engine Initializer --- 
def call_session_engine(source_system=None, database_name=None):

    rds_secret_name = os.environ["RDS_SECRETS_MANAGER_ID"]
    region_name = os.environ["AWS_REGION"]
    rds_host_nm = os.environ['RDS_HOST']

    if database_name == 'ref_data':
        rds_db_nm = os.environ['RDS_REF_DB_NAME']
    elif database_name == 'mdm_raw':
        rds_db_nm = os.environ['RDS_RAW_DB_NAME']
    elif database_name == 'mdm_refined':
        rds_db_nm = os.environ['RDS_REFINED_DB_NAME']
    else:
        rds_db_nm = os.environ['RDS_DB_NAME']

    if source_system and source_system.lower() == 'as400_affprd':
        as400_secret_name = os.environ["AS400_AFF_SECRETS_MANAGER_ID"]
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    elif source_system and source_system.lower() == 'as400_kkins':
        as400_secret_name = os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        as400_engine = get_as400_db_session(as400_secret_name, region_name)
        return as400_engine

    if database_name:
        session = get_rds_db_session(rds_secret_name, region_name, rds_host_nm, rds_db_nm)
        return session


# --- Global Sessions ---
session = call_session_engine(database_name=pnc_db)
as400_engine_aff = call_session_engine(source_system='AS400_AFFPRD')
as400_engine_kkins = call_session_engine(source_system='as400_kkins')
as400_engine_attorney = call_session_engine(source_system='as400_attorney')


# --- Lookup Customer from AS400 ---
def lookup_as400_customer_contact(config=None, id=None):
    """
    Query AS400 systems (AFF / KKINS) for customer record.
    You can adjust the column mappings as per AS400 schema.
    """
    source_system = config.get("source_system")
    df = None

    if source_system and source_system.lower() == "as400_affprd":
        df = pd.read_sql(f"""
            SELECT *
            FROM (
                SELECT DISTINCT
                    CAST(NULL AS varchar(36)) AS df_customer_contact_id,
                    cu.CUSKEY AS source_customer_contact_id,
                    'AS400_AFFPRD' AS source_system,
                    CAST(NULL AS varchar(36)) AS df_customer_id,
                    CAST(NULL AS varchar(36)) AS df_source_system_id,
                    CAST(NULL AS varchar(36)) AS mdm_customer_id,
                    CAST(NULL AS timestamp) AS created_date,
                    CAST(NULL AS timestamp) AS modified_date
                FROM ADGDTADV.NSOCVGP AS pol
                INNER JOIN ADGDTADV.HCPCSTP AS cust
                    ON pol.customer_no = cust.CUSTNO
                INNER JOIN ADGDTADV.HCPCUSP AS cu
                    ON cust.CSTKEY = cu.CUSKEY
                WHERE pol.POLICY_TYPE = 'I'
            ) t
            WHERE t.source_customer_contact_id = '{id}';
        """, con=as400_engine_aff)

    elif source_system and source_system.lower() == "as400_kkins":
        df = pd.read_sql(f"""
            SELECT DISTINCT 
                CAST(null as varchar(255)) as df_customer_contact_id,
                an.AC_ACCOUNT_NBR AS source_customer_contact_id,
                an.AC_ACCOUNT_NBR AS source_customer_id,
                'AS400_KKINS' AS source_system,
                CAST(null as varchar(255)) as mdm_customer_contact_id,
                CAST(null as timestamp) as created_at,
                CAST(null as timestamp) as modified_at
            FROM PLCYPROD.ANZ002 an
            INNER JOIN USERLIB.PLCYDATA pl
                ON an.AC_ACCOUNT_NBR = pl.AC_ACCOUNT_NBR
            WHERE an.AC_ACCOUNT_NBR = '{id}';
        """, con=as400_engine_kkins)

    if df is not None and len(df) > 0:
        return df.to_dict("records")[0]
    return None

# --- Core Consumer ---
def consume_lambda(config=None):

    now = datetime.now()
    start_ts = datetime.timestamp(now)

    try:
        # Parse configs
        if isinstance(config, dict):
            config_list = [config]
        elif isinstance(config, list):
            config_list = config
        else:
            config_list = json.loads(str(config))
            if not isinstance(config_list, list):
                config_list = [config_list]

        for config_dict in config_list:
            contact_id = config_dict.get("source_customer_contact_id")
            source_system = config_dict.get("source_system", "")

            # AS400 Lookup
            as400_contact_dict = lookup_as400_customer_contact(config_dict, contact_id)

            if not as400_contact_dict:
                logger.info(f"No AS400 customer_contact record found for {contact_id}")
                continue

            logger.info(f"AS400 Customer Contact Data Fetched")

            # ---------------------------------------------------------------
            # 2. FK Lookup: Source System (NO insert)
            # ---------------------------------------------------------------

            ss_rec = session.query(Source_System).filter(
                Source_System.source_system == as400_contact_dict.get("source_system")).first()
            if not ss_rec:
                logger.info(f"Source system not found — skipping agency_contact {contact_id}"
                    )
                continue

            df_source_system_id = ss_rec.df_source_system_id
            as400_contact_dict["df_source_system_id"] = df_source_system_id

            customer_rec = session.query(Customer).filter(
                Customer.source_customer_id == as400_contact_dict.get("source_customer_id"),
                Customer.df_source_system_id == as400_contact_dict.get("df_source_system_id")).first()
            if not customer_rec:
                logger.info(f"Customer not found — skipping customer {contact_id}"
                    )
                continue

            df_customer_id = customer_rec.df_customer_id
            as400_contact_dict["df_customer_id"] = df_customer_id
            # ---------------------------------------------------------------
            # 4. EXISTING CUSTOMER_CONTACT CHECK
            # ---------------------------------------------------------------

            existing_contact = session.query(Customer_Contact).filter(
                Customer_Contact.source_customer_contact_id == as400_contact_dict["source_customer_contact_id"],
                Customer_Contact.df_source_system_id == df_source_system_id
            ).first()

            # # ---------------------------------------------------------------
            # # 5. INSERT LOGIC
            # # ---------------------------------------------------------------


            if not existing_contact:

                df_customer_contact_id = generate_uuid(
                    as400_contact_dict["source_customer_contact_id"], 
                    str(df_source_system_id)
                )

                as400_contact_dict["df_customer_contact_id"] = df_customer_contact_id

                # IMPORTANT: created_at & modified_at for INSERT
                as400_contact_dict["created_at"] = (
                    convert_timestamps(as400_contact_dict.get("created_date"))
                    if as400_contact_dict.get("created_date")
                    else datetime.now()
                )

                as400_contact_dict["modified_at"] = (
                    convert_timestamps(as400_contact_dict.get("modified_date"))
                    if as400_contact_dict.get("modified_date")
                    else datetime.now()
                )

                session.add(
                    Customer_Contact.from_dict(cls=Customer_Contact, d=as400_contact_dict)
                )
                session.commit()

                logger.info(f"Inserted Agency Contact {as400_contact_dict["source_customer_contact_id"]}"
                    )

            # ---------------------------------------------------------------
            # 6. UPDATE LOGIC
            # ---------------------------------------------------------------

            else:

                as400_contact_dict["df_customer_contact_id"] = existing_contact.df_customer_contact_id

                # Only update modified_at
                as400_contact_dict["modified_at"] = datetime.now()

                # NEVER update created_at
                if "created_at" in as400_contact_dict:
                    del as400_contact_dict["created_at"]

                q = session.query(Customer_Contact).filter(
                    Customer_Contact.df_customer_contact_id == existing_contact.df_customer_contact_id
                )

                q.update(query_update_dict(obj=Customer_Contact, dict=as400_contact_dict))
                session.commit()

                logger.info(f"Updated Agency Contact {as400_contact_dict["source_customer_contact_id"]}"
                    )

        end_ts = datetime.timestamp(datetime.now())
        return {"execution_time_sec": end_ts - start_ts}

    except SQLAlchemyError as e:
        session.rollback()
        logger.info(f"DB Error", api_response=str(e))
        raise e

# --- Lambda Handler ---
def handle(event, context):
    start_time = time.time()
    failures = []

    for record in event.get("Records", []):
        logger.info(record)
        payload = record.get("body")
        try:

            consume_lambda(config=payload)   # sync call is correct
        except Exception:
            message_id = record.get("messageId")
            if message_id:
                failures.append({"itemIdentifier": message_id})
            else:
                logger.error("Record missing messageId: %s", record, exc_info=True)

    end_time = time.time()
    logger.info("Lambda handle duration: %.3f seconds", end_time - start_time)

    return {
        "batchItemFailures": failures
    }

# if __name__ == "__main__":
#     # sample test event (adjust source_system and contact id as needed)
#     test_event = {
#         "Records": [
#             {"body": '{"source_customer_contact_id":"313389","source_system":"AS400_KKINS"}'}
#         ]
#     }
#     handle(test_event, None)





Underwriter


import os
import logging
import json
import time
import requests
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import  Source_System_Raw as Source_System, Underwriter
from df_database_models.db_utils import  generate_uuid, convert_timestamps, generate_uuid, query_update_dict, multi_filter_get_record
from secrets_manager import get_secret
from datetime import datetime
import pandas as pd
import asyncio
from adf_pyutils.clm_wrapper import common_logger
 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
pnc_db = os.environ['RDS_DB_NAME']
ref_db = os.environ['RDS_REF_DB_NAME']
mdm_raw_db = os.environ['RDS_RAW_DB_NAME']
mdm_refined_db = os.environ['RDS_REFINED_DB_NAME']
 
 
async def log_msg(func,**kwargs):
    await asyncio.to_thread(func,**kwargs)
 
def call_session_engine(source_system=None, database_name=None):
 
    rds_secret_name=os.environ["RDS_SECRETS_MANAGER_ID"]
    region_name=os.environ["AWS_REGION"]
    rds_host_nm=os.environ['RDS_HOST']
 
    if database_name == 'ref_data':
        rds_db_nm=os.environ['RDS_REF_DB_NAME']
    elif database_name == 'mdm_raw':
        rds_db_nm=os.environ['RDS_RAW_DB_NAME']
    elif database_name == 'mdm_refined':
        rds_db_nm=os.environ['RDS_REFINED_DB_NAME']
    else:
        rds_db_nm=os.environ['RDS_DB_NAME']
 
 
    if source_system == 'as400_affprd':
        #Calling the as400 engine to establish a connection to PAS Source System - as400_AFF
        as400_secret_name=os.environ["AS400_AFF_SECRETS_MANAGER_ID"]
        as400_engine=get_as400_db_session(as400_secret_name, region_name)
        return as400_engine
 
    elif source_system == 'as400_kkins':
        #Calling the as400 engine to establish a connection to PAS Source System - as400_AUM
        as400_secret_name=os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        as400_engine=get_as400_db_session(as400_secret_name, region_name)
        return as400_engine
 
    elif source_system == 'as400_attorney':
        #Calling the as400 engine to establish a connection to PAS Source System - as400_AUM
        as400_secret_name=os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        as400_engine=get_as400_db_session(as400_secret_name, region_name)
        return as400_engine
 
    if database_name:
        #Calling the Db session Object to establish a connection to Data Foundation Schema
        session=get_rds_db_session(rds_secret_name,region_name,rds_host_nm,rds_db_nm)
 
        return session
 
session = call_session_engine(database_name=pnc_db)
as400_engine_aff = call_session_engine(source_system='as400_affprd')
as400_engine_kkins = call_session_engine(source_system='as400_kkins')
as400_engine_attorney = call_session_engine(source_system='as400_attorney')
 
# lookup into as400 aff
def lookup_as400(config=None, id=None):
 
    source_system = config['source_system']
    if source_system:
        if(source_system.lower() == 'as400_affprd'):
                df = pd.read_sql(f"""
                    SELECT DISTINCT
                    SASRP# AS source_underwriter_id,
                    'AS400_AFFPRD' as source_system,
                    CONCAT(CONCAT(LTRIM(RTRIM(SASRNM)), ' '), LTRIM(RTRIM(SASRNL))) AS underwriter_name
                    FROM ADGDTADV.ADGSALP
                    WHERE SAACTV = 'Y'
                    AND SASRP# = {id};
                    """, con=as400_engine_aff)
 
        elif(source_system.lower() == 'as400_attorney'):
                df = pd.read_sql(f"""
                   
                    """, con=as400_engine_attorney)
 
        elif(source_system.lower() == 'as400_kkins'):
                df = pd.read_sql(f"""
                   SELECT DISTINCT PL_underwriter_id AS source_underwriter_id,
                    UN_NAME AS name,
                    'AS400_KKINS' as source_system
                    FROM PLCYADMIN.AEZ113
                    WHERE PL_underwriter_id = '{id}';
                    """, con=as400_engine_kkins)
    else:
        df=None
 
    if(len(df)>0):
        return df.to_dict('records')
    else:
        return None
 
def consume_lambda(config=None):
 
    logger.info(f'consume lambda function invoking')
    now = datetime.now()
    start_ts = datetime.timestamp(now)
 
    logger.info(f'Processing to DB @ {now} | {datetime.timestamp(now)}'
        )
 
    try:
        logger.info(f'Config {config}')
 
        config_dicts = config if type(config) is dict else json.loads(str(config))
 
        if type(config_dicts) is not list:
            config_dicts = [config_dicts]  # Ensure config_dicts is a list
 
        for idx, config_dict in enumerate(config_dicts):
 
            underwriter_id = config_dict['source_underwriter_id']  # Get the underwriter from config data
            source_system = config_dict['source_system'].lower()
 
            if (id):
                fk_flag = 1
 
                as400_underwriter_summary_dicts = lookup_as400(config_dict, underwriter_id)

                for as400_underwriter_summary_dict in as400_underwriter_summary_dicts:

                    if (as400_underwriter_summary_dict):
                        logger.info(f'Initial {source_system} underwriter Summary dict: {as400_underwriter_summary_dict}')
    
                        # ---------------------------------------------------------------
                        #  FK Lookup: Source System (NO insert)
                        # ---------------------------------------------------------------
        
                        ss_rec = session.query(Source_System).filter(
                            Source_System.source_system == as400_underwriter_summary_dict.get("source_system")).first()
                        if not ss_rec:
                            logger.info(f"Source system not found — skipping underwriter"
                                )
                            continue
        
                        df_source_system_id = ss_rec.source_system_id
                        as400_underwriter_summary_dict["df_source_system_id"] = df_source_system_id
    
                        source_underwriter_id = as400_underwriter_summary_dict.get("source_underwriter_id")
                        df_source_system_id = as400_underwriter_summary_dict.get("df_source_system_id")
        
                        underwriter_record = session.query(Underwriter).filter(
                            Underwriter.source_underwriter_id == source_underwriter_id,
                            Underwriter.df_source_system_id == df_source_system_id
                            ).first()
    
                        # ---------------------------------------------------------------
                        #  INSERT LOGIC
                        # ---------------------------------------------------------------
    
                        if not underwriter_record:
    
                            df_underwriter_id = generate_uuid(
                                str(source_underwriter_id),
                                str(df_source_system_id)
                            )
    
                            as400_underwriter_summary_dict["df_underwriter_id"] = df_underwriter_id
    
                            # IMPORTANT: created_at & modified_at for INSERT
                            as400_underwriter_summary_dict["created_at"] = (
                                convert_timestamps(as400_underwriter_summary_dict.get("created_date"))
                                if as400_underwriter_summary_dict.get("created_date")
                                else datetime.now()
                            )
    
                            as400_underwriter_summary_dict["modified_at"] = (
                                convert_timestamps(as400_underwriter_summary_dict.get("modified_date"))
                                if as400_underwriter_summary_dict.get("modified_date")
                                else datetime.now()
                            )

                            session.add(
                                Underwriter.from_dict(cls=Underwriter, d=as400_underwriter_summary_dict)
                            )
                            session.commit()
    
                            logger.info(f"Inserted Agency Contact {source_underwriter_id}"
                                )
    
                        # ---------------------------------------------------------------
                        #  UPDATE LOGIC
                        # ---------------------------------------------------------------
    
                        else:
    
                            as400_underwriter_summary_dict["df_agency_contact_id"] = underwriter_record.df_underwriter_id
    
                            # Only update modified_at
                            as400_underwriter_summary_dict["modified_at"] = datetime.now()
    
                            # NEVER update created_at
                            if "created_at" in as400_underwriter_summary_dict:
                                del as400_underwriter_summary_dict["created_at"]
    
                            q = session.query(Underwriter).filter(
                                Underwriter.df_underwriter_id == underwriter_record.df_underwriter_id
                            )
    
                            q.update(query_update_dict(obj=Underwriter, dict=as400_underwriter_summary_dict))
                            session.commit()
    
                            logger.info(f"Updated Agency Contact {source_underwriter_id}"
                                )

    
                end_ts = datetime.timestamp(datetime.now())
                return {"execution_time_sec": end_ts - start_ts}
 
    except SQLAlchemyError as e:
        logger.info(f'Error'
            )
        session.rollback()
        raise e
 
# ---------------- Lambda Handler ---------------- #
def handle(event, context):
    start_time = time.time()
    failures = []

    for record in event.get("Records", []):
        logger.info(record)
        payload = record.get("body")
        try:
            consume_lambda(config=payload)   # sync call is correct
        except Exception:
            message_id = record.get("messageId")
            if message_id:
                failures.append({"itemIdentifier": message_id})
            else:
                logger.error("Record missing messageId: %s", record, exc_info=True)

    end_time = time.time()
    logger.info("Lambda handle duration: %.3f seconds", end_time - start_time)

    return {
        "batchItemFailures": failures
    }

 
 
# if __name__ == '__main__':
#     handle({'Records': [{'body': '[{"source_underwriter_id": "AGB", "source_system": "AS400_KKINS"}]'}]}, None)





Carrier

import os
import json
import time, logging
import requests
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from df_database_models.db_conn import get_rds_db_session, get_as400_db_session
from df_database_models.models import Agency_Contact_Raw as AgencyContact, Source_System, Carrier
from df_database_models.db_utils import  generate_uuid, convert_timestamps, generate_uuid, query_update_dict, multi_filter_get_record
from secrets_manager import get_secret
from datetime import datetime
import pandas as pd
import asyncio
from adf_pyutils.clm_wrapper import common_logger

logger = logging.getLogger()
logger.setLevel(logging.INFO)

pnc_db = os.environ['RDS_DB_NAME']
ref_db = os.environ['RDS_REF_DB_NAME']
mdm_raw_db = os.environ['RDS_RAW_DB_NAME']
mdm_refined_db = os.environ['RDS_REFINED_DB_NAME']

# Fetch SQS producer Parameters from aws secret manager

async def log_msg(func,**kwargs):
    await asyncio.to_thread(func,**kwargs)

def call_session_engine(source_system=None, database_name=None):
 
    rds_secret_name=os.environ["RDS_SECRETS_MANAGER_ID"]
    region_name=os.environ["AWS_REGION"]
    rds_host_nm=os.environ['RDS_HOST']
 
    if database_name == 'ref_data':
        rds_db_nm=os.environ['RDS_REF_DB_NAME']
    elif database_name == 'mdm_raw':
        rds_db_nm=os.environ['RDS_RAW_DB_NAME']
    elif database_name == 'mdm_refined':
        rds_db_nm=os.environ['RDS_REFINED_DB_NAME']
    else:
        rds_db_nm=os.environ['RDS_DB_NAME']
 
    if source_system and source_system.lower() == 'as400_affprd':
        #Calling the as400 engine to establish a connection to PAS Source System - as400_AFF
        as400_secret_name=os.environ["AS400_AFF_SECRETS_MANAGER_ID"]
        as400_engine=get_as400_db_session(as400_secret_name, region_name)
        return as400_engine
 
    elif source_system and source_system.lower() == 'as400_kkins':
        #Calling the as400 engine to establish a connection to PAS Source System - as400_AUM
        as400_secret_name=os.environ["AS400_KKINS_SECRETS_MANAGER_ID"]
        as400_engine=get_as400_db_session(as400_secret_name, region_name)
        return as400_engine
 
    if database_name:
        #Calling the Db session Object to establish a connection to Data Foundation Schema
        session=get_rds_db_session(rds_secret_name,region_name,rds_host_nm,rds_db_nm)
 
        return session

session = call_session_engine(database_name=pnc_db)
as400_engine_aff = call_session_engine(source_system='as400_affprd')
as400_engine_kkins = call_session_engine(source_system='as400_kkins')


def lookup_as400(config=None, id=None):

    source_system = config['source_system']
    if source_system:
        if(source_system.lower() == 'as400_affprd'):
                df = pd.read_sql(f"""
                    SELECT MCCR_COMPANY_CODE AS source_carrier_id
                    ,COFL01 AS carrier_name
                    ,'AS400_AFFPRD' AS source_system
                    FROM ADGDTAPR.NSPSRCP
                    WHERE MCCR_COMPANY_CODE = '{id}'
                    """, con=as400_engine_aff)


        elif(source_system.lower() == 'as400_kkins'):
                df = pd.read_sql(f"""
                    SELECT DISTINCT KKPREF + '-' + KKCODE AS source_carrier_id,
                        KKDESC AS carrier_name,
                        'AS400_KKINS' as source_system
                    FROM USERLIB.KKMISC
                    WHERE KKPREF = 'PCM' and KKPREF + '-' + KKCODE = '{id}';
                    """, con=as400_engine_kkins)
    else:
        df=None

    if(len(df)>0):
        return df.to_dict('records')
    else:
        return None

async def consume_lambda(config=None):
    logger.info(f'consume lambda function invoking')
    now = datetime.now()
    start_timestamp = datetime.timestamp(now)
    logger.info(f'Processing to DB @ {now} | {datetime.timestamp(now)}')

    try:
        logger.info(f'Config {config}')
        config_dicts = config if type(config) is dict else json.loads(str(config))
        if type(config_dicts) == list:
            pass
        else:
            config_dicts = [config_dicts] # Ensure config_dicts is a list
        for config_dict in config_dicts:
            carrier_id = config_dict['source_carrier_id'] # Get the carrier from config data 
            source_system = config_dict['source_system'].lower()
            if(id):
                fk_flag = 1

                as400_carrier_summary_dicts = lookup_as400(config_dict, carrier_id) #define dict for lookup data
                for as400_carrier_summary_dict in as400_carrier_summary_dicts:

                    if(as400_carrier_summary_dict):
                        logger.info(f'Initial {source_system} Carrier Summary dict:{as400_carrier_summary_dict}')

                        #Fetch Source SyStem Id from Data Foundation
                        source_system = as400_carrier_summary_dict.get("source_system")
                        source_system_record = (query.first() if (query := multi_filter_get_record(session,model=Source_System,source_system=source_system)) is not None else None)

                        if source_system_record:
                            as400_carrier_summary_dict['df_source_system_id'] = source_system_record.df_source_system_id

                        source_carrier_id = as400_carrier_summary_dict.get("source_carrier_id")
                        df_source_system_id = as400_carrier_summary_dict.get("df_source_system_id")

                        carrier_record = multi_filter_get_record(session, model = Carrier, source_carrier_id = source_carrier_id, df_source_system_id = df_source_system_id)
                        self_carrier = (carrier_record.first() if carrier_record is not None else None)

                        logger.info(f'self carrier - {self_carrier}')
                        logger.info(f'Changed {source_system} Carrier Summary dict: {as400_carrier_summary_dict}')

                        if self_carrier is None:
                            logger.info(f'Carrier does not exist in Data Foundation')
                            as400_carrier_summary_dict['df_carrier_id'] = generate_uuid(
                                str(as400_carrier_summary_dict['source_carrier_id']) + 
                                str(as400_carrier_summary_dict['df_source_system_id'])
                            )
                            logger.info(f'Insert {source_system} Carrier Summary dict: {as400_carrier_summary_dict}')
                            session.add(Carrier.from_dict(cls = Carrier, d = as400_carrier_summary_dict))
                            session.commit()
                            logger.info(f'Inserted carrier {carrier_id}')
                        else:
                            as400_carrier_summary_dict["df_carrier_id"] = self_carrier.df_carrier_id
                            as400_carrier_summary_dict["created_at"] = self_carrier.created_at
                            as400_carrier_summary_dict["modified_at"] = datetime.now()

                            q = session.query(Carrier).filter(
                                Carrier.df_carrier_id == self_carrier.df_carrier_id
                            )
                            q.update(query_update_dict(obj=Carrier, dict=as400_carrier_summary_dict))
                            session.commit()
                    else:
                        error_log = {
                            "df_line_item_id" : id,
                            "error_message" : "No record found after lookup" 
                        }

                        logger.info(f"No record found for LineItem {carrier_id}")        
        now = datetime.now()
        end_timestamp = datetime.timestamp(now)
        logger.info(f'execution_time: {end_timestamp} - {start_timestamp}')
    except SQLAlchemyError as e:
        logger.info(f'Error',api_response=e)
        session.rollback()
        raise e



def handle(event, context):
    start_time = time.time()
    failures = []

    for record in event.get("Records", []):
        logger.info(record)
        payload = record.get("body")
        try:

            consume_lambda(config=payload)   # sync call is correct
        except Exception:
            message_id = record.get("messageId")
            if message_id:
                failures.append({"itemIdentifier": message_id})
            else:
                logger.error("Record missing messageId: %s", record, exc_info=True)

    end_time = time.time()
    logger.info("Lambda handle duration: %.3f seconds", end_time - start_time)

    return {
        "batchItemFailures": failures
    }


# if __name__ == '__main__':
#     handle({'Records': [{'body': '[{"source_carrier_id": "PCM-002", "source_system": "AS400_KKINS"}]'}]}, None)



