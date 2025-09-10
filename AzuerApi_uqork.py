import os
import json
import time
import logging
from sqlalchemy.exc import SQLAlchemyError

from df_database_models.db_conn import get_rds_db_session
from df_database_models.models import (
    Source_System, Customer, Submission, Quote, Product,
    Submission_Status, Submission_Type, Quote_Status, Quote_Type
)

# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def consume_unqork(session, config=None):
    try:
        config_dict = config if isinstance(config, dict) else json.loads(str(config))

        # 1. Source System
        source_system = session.query(Source_System).filter_by(source_system="Unqork").first()
        if not source_system:
            source_system = Source_System(source_system="Unqork")
            session.add(source_system)
            session.commit()

        # 2. Product
        product = session.query(Product).filter_by(
            program_name=config_dict.get("program_name"),
            product_name=config_dict.get("product_name")
        ).first()
        if not product:
            product = Product(
                program_name=config_dict.get("program_name"),
                product_name=config_dict.get("product_name")
            )
            session.add(product)
            session.commit()

        # 3. Submission Status
        submission_status = session.query(Submission_Status).filter_by(
            submission_status=config_dict.get("submission_status")
        ).first()
        if not submission_status:
            submission_status = Submission_Status(
                submission_status=config_dict.get("submission_status")
            )
            session.add(submission_status)
            session.commit()

        # 4. Submission Type
        submission_type = session.query(Submission_Type).filter_by(
            submission_type=config_dict.get("submission_type")
        ).first()
        if not submission_type:
            submission_type = Submission_Type(
                submission_type=config_dict.get("submission_type")
            )
            session.add(submission_type)
            session.commit()

        # 5. Customer
        customer = session.query(Customer).filter_by(
            source_customer_id=config_dict["source_customer_id"]
        ).first()
        if not customer:
            customer = Customer(
                source_customer_id=config_dict["source_customer_id"],
                df_source_system_id=source_system.df_source_system_id
            )
            session.add(customer)
            session.commit()

        # 6. Submission
        submission = session.query(Submission).filter_by(
            source_submission_id=config_dict["submission_id"]
        ).first()

        if not submission:
            submission = Submission(
                source_submission_id=config_dict["submission_id"],
                df_customer_id=customer.df_customer_id,
                product_id=product.product_id,
                df_source_system_id=source_system.df_source_system_id,
                df_submission_status_id=submission_status.df_submission_status_id,
                df_submission_type_id=submission_type.df_submission_type_id,
                effective_date=config_dict.get("effective_date"),
                expiration_date=config_dict.get("expiration_date")
            )
            session.add(submission)
            session.commit()
        else:
            submission.effective_date = config_dict.get("effective_date")
            submission.expiration_date = config_dict.get("expiration_date")
            session.commit()

        # 7. Quotes
        for proposal in config_dict.get("proposal_detail", []):
            # Quote Status
            quote_status = session.query(Quote_Status).filter_by(
                quote_status=proposal.get("proposal_status")
            ).first()
            if not quote_status:
                quote_status = Quote_Status(
                    quote_status=proposal.get("proposal_status")
                )
                session.add(quote_status)
                session.commit()

            # Quote Type
            quote_type = session.query(Quote_Type).filter_by(
                quote_type=proposal.get("proposal_type")
            ).first()
            if not quote_type:
                quote_type = Quote_Type(
                    quote_type=proposal.get("proposal_type")
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
                    df_quote_status_id=quote_status.df_quote_status_id,
                    df_quote_type_id=quote_type.df_quote_type_id,
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


# Lambda-style handler
def handle(event, context):
    start_time = time.time()
    session = get_rds_db_session(
        os.environ["RDS_SECRETS_MANAGER_ID"],
        os.environ["AWS_REGION"],
        os.environ["RDS_HOST"],
        os.environ["RDS_DB_NAME"]
    )

    for record in event["Records"]:
        payload = json.loads(record["body"])
        consume_unqork(session, config=payload)

    session.close()
    end_time = time.time()
    return {"execution_time_sec": end_time - start_time}




{
  "submission_id": "SUB12345",
  "source_customer_id": "CUST7890",
  "customer_name": "Acme Corp",
  "program_name": "Small Business",
  "product_name": "General Liability",
  "submission_status": "New",
  "submission_type": "Online",
  "effective_date": "2025-09-10T00:00:00",
  "expiration_date": "2026-09-09T23:59:59",
  "proposal_detail": [
    {
      "proposal_id": "PROP001",
      "proposal_status": "Pending",
      "proposal_type": "Initial",
      "proposal_effective_date": "2025-09-15T00:00:00",
      "proposal_expiration_date": "2026-09-14T23:59:59"
    },
    {
      "proposal_id": "PROP002",
      "proposal_status": "Approved",
      "proposal_type": "Renewal",
      "proposal_effective_date": "2025-09-20T00:00:00",
      "proposal_expiration_date": "2026-09-19T23:59:59"
    }
  ]
}
