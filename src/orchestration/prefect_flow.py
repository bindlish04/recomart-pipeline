from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from src.common.logger import get_logger

logger = get_logger("prefect_flow")

# --- Each task calls your existing modules ---

@task(retries=2, retry_delay_seconds=10)
def ingest_csv():
    logger.info("Starting CSV ingestion...")
    from src.ingestion.ingest_interactions_csv import main as run
    run()
    logger.info("CSV ingestion done.")

@task(retries=2, retry_delay_seconds=10)
def ingest_api():
    logger.info("Starting API ingestion...")
    from src.ingestion.ingest_products_api import main as run
    run()
    logger.info("API ingestion done.")

@task(retries=1, retry_delay_seconds=5)
def validate_interactions():
    logger.info("Validating interactions...")
    from src.validation.validate_interactions import main as run
    run()
    logger.info("Validation interactions done.")

@task(retries=1, retry_delay_seconds=5)
def validate_products():
    logger.info("Validating products...")
    from src.validation.validate_products import main as run
    run()
    logger.info("Validation products done.")

@task
def generate_dq_pdf():
    logger.info("Generating Data Quality Report PDF...")
    from src.validation.generate_data_quality_pdf import main as run
    run()
    logger.info("DQ PDF generated.")

@task
def prepare_and_eda():
    logger.info("Running preparation + EDA...")
    from src.preparation.clean_and_eda import main as run
    run()
    logger.info("Preparation + EDA done.")

@task
def build_features():
    logger.info("Building features + warehouse tables...")
    from src.transformation.build_features import main as run
    run()
    logger.info("Features built.")

@task
def train_model():
    logger.info("Training model...")
    from src.modeling.train_recommender import main as run
    run()
    logger.info("Model training done.")

@task
def evaluate_model():
    logger.info("Evaluating model + logging to MLflow...")
    from src.modeling.evaluate import main as run
    run()
    logger.info("Model evaluation done.")

@flow(name="recomart-end-to-end-pipeline")
def recomart_pipeline():
    # Ingestion can run in parallel
    ingest_csv_future = ingest_csv.submit()
    ingest_api_future = ingest_api.submit()

    # Wait for ingestion tasks to finish before validation
    ingest_csv_future.result()
    ingest_api_future.result()

    validate_interactions_future = validate_interactions.submit()
    validate_products_future = validate_products.submit()

    validate_interactions_future.result()
    validate_products_future.result()

    generate_dq_pdf()

    prepare_and_eda()
    build_features()

    train_model()
    evaluate_model()

if __name__ == "__main__":
    recomart_pipeline()
