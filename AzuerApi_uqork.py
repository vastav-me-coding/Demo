import os
import json
import time
import logging
import requests
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, MetaData
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import SQLAlchemyError
from secrets_manager import get_secret

# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database Connection
Base = declarative_base()
db_secret = json.loads(get_secret(
    secret_name=os.environ["RDS_SECRETS_MANAGER_ID"],
    region_name=os.environ["AWS_REGION"]
))

rds_host = os.environ['RDS_HOST']
rds_db_name = os.environ['RDS_DB_NAME']
ods_conn_str = f"mysql+pymysql://{db_secret['username']}:{db_secret['password']}@{rds_host}/{rds_db_name}"

engine = create_engine(ods_conn_str)
session = Session(engine)
meta = MetaData(engine)

# ORM classes
class SourceSystem(Base):
    __tablename__ = "source_system"
    df_source_system_id = Column(Integer, primary_key=True)
    source_system = Column(String)
    name = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)


class Customer(Base):
    __tablename__ = "customer"
    df_customer_id = Column(Integer, primary_key=True)
    source_customer_id = Column(String)
    customer_name = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)


class Submission(Base):
    __tablename__ = "submission"
    df_submission_id = Column(Integer, primary_key=True)
    source_submission_id = Column(String)
    df_customer_id = Column(Integer)
    product_id = Column(Integer)
    df_submission_status_id = Column(Integer)
    df_submission_type_id = Column(Integer)
    df_source_system_id = Column(Integer)
    effective_date = Column(DateTime)
    expiration_date = Column(DateTime)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)


class Quote(Base):
    __tablename__ = "quote"
    df_quote_id = Column(Integer, primary_key=True)
    source_quote_id = Column(String)
    df_submission_id = Column(Integer)
    df_quote_status_id = Column(Integer)
    df_quote_type_id = Column(Integer)
    df_source_system_id = Column(Integer)
    effective_date = Column(DateTime)
    expiration_date = Column(DateTime)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)


class Product(Base):
    __tablename__ = "product"
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String)
    program_name = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)

class SubmissionStatus(Base):
    __tablename__ = "submission_status"
    df_submission_status_id = Column(Integer, primary_key=True)
    submission_status = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)

class SubmissionType(Base):
    __tablename__ = "submission_type"
    df_submission_type_id = Column(Integer, primary_key=True)
    submission_type = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)

class QuoteStatus(Base):
    __tablename__ = "quote_status"
    df_quote_status_id = Column(Integer, primary_key=True)
    quote_status = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)

class QuoteType(Base):
    __tablename__ = "quote_type"
    df_quote_type_id = Column(Integer, primary_key=True)
    quote_type = Column(String)
    created_date = Column(DateTime)
    modified_date = Column(DateTime)

def consume_unqork(config=None):
    try:
        config_dict = config if isinstance(config, dict) else json.loads(str(config))

        # Source System
        source_system = session.query(SourceSystem).filter_by(source_system="Unqork").first()
        if not source_system:
            source_system = SourceSystem(source_system="Unqork", name="Unqork API")
            session.add(source_system)
            session.commit()

        # product
        product = session.query(Product).filter_by(
            program_name = config_dict.get("program_name"),
            product_name = config_dict.get("product_name")
            ).first()
        if not product:
            product = product(
            program_name = config_dict.get("program_name"),
            product_name = config_dict.get("product_name")
            )
            session.add(product)
            session.commit()

        # Submission Status
        submission_status = session.query(SubmissionStatus).filter_by(
            submission_status = config_dict.get("submission_status"),
            ).first()
        if not submission_status:
            submission_status = submission_status(
            config_dict.get("submission_status")
            )
            session.add(product)
            session.commit()

        # Submission Type
        submission_type = session.query(SubmissionType).filter_by(
            submission_type = config_dict.get("submission_type"),
            ).first()
        if not submission_type:
            submission_type = submission_type(
            config_dict.get("submission_type")
            )
            session.add(product)
            session.commit()

        # Customer
        customer = session.query(Customer).filter_by(
            source_customer_id=config_dict["source_customer_id"]
        ).first()
        if not customer:
            customer = Customer(
                source_customer_id=config_dict["source_customer_id"],
                customer_name=config_dict.get("customer_name")
            )
            session.add(customer)
            session.commit()

        # Submission
        submission = session.query(Submission).filter_by(
            source_submission_id=config_dict["submission_id"]
        ).first()

        if not submission:
            submission = Submission(
                source_submission_id=config_dict["submission_id"],
                df_customer_id=customer.df_customer_id,
                product_id = product.product_id,
                df_source_system_id=source_system.df_source_system_id,
                df_submission_status_id = submission_status.df_submission_status_id,
                df_submission_type_id = submission_type.df_submission_type_id,
                effective_date=config_dict.get("effective_date"),
                expiration_date=config_dict.get("expiration_date")
            )
            session.add(submission)
            session.commit()
        else:
            submission.effective_date = config_dict.get("effective_date")
            submission.expiration_date = config_dict.get("expiration_date")
            session.commit()

        # Quotes proposal_detail
        for proposal in config_dict.get("proposal_detail", []):

            # Quote Status
            quote_status = session.query(QuoteStatus).filter_by(
                quote_status=proposal.get["proposal_status"]
            ).first()
            if not quote_status:
                quote_status = QuoteStatus(
                    quote_status = proposal.get["proposal_status"]
                )
                session.add(quote_status)
                session.commit()

            # Quote Type
            quote_type = session.query(QuoteType).filter_by(
                quote_type=proposal.get["proposal_status"]
            ).first()
            if not quote_type:
                quote_type = QuoteType(
                    quote_type = proposal.get["proposal_status"]
                )
                session.add(quote_type)
                session.commit()

            quote = session.query(Quote).filter_by(
                source_quote_id=proposal["proposal_id"]
            ).first()

            if not quote:
                quote = Quote(
                    source_quote_id=proposal["proposal_id"],
                    df_submission_id=submission.df_submission_id,
                    df_source_system_id=source_system.df_source_system_id,
                    df_quote_status_id =  quote_status.df_quote_status_id,
                    df_quote_type_id = quote_type.df_quote_type_id,
                    effective_date=proposal.get("proposal_effective_date"),
                    expiration_date=proposal.get("proposal_expiration_date")
                )
                session.add(quote)
                session.commit()
            else:
                quote.effective_date = proposal.get("proposal_effective_date")
                quote.expiration_date = proposal.get("proposal_expiration_date")
                session.commit()

        
        return {"status": "success"}

    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"DB Error: {e}")
        raise e


# Handler Function
def handle(event, context):
    start_time = time.time()

    for record in event['Records']:
        payload = record["body"]
        consume_unqork(config=payload)

    end_time = time.time()
    return {"execution_time_sec": end_time - start_time}