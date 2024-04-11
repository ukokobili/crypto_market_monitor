import datetime
import logging
import sys

<<<<<<< HEAD
=======
sys.path.insert(0, './logs/')
from config import log_config
>>>>>>> origin/ci
from etl.extract import get_exchange_data
from etl.load import write_to_motherduck_from_data_frame
from etl.transform import transform_exchange_data

sys.path.insert(0, './logs/')
from config import log_config  # noqa

log_config()
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    try:
        # Pipeline start time
        pipeline_start_time = datetime.datetime.now()

        data = get_exchange_data()
        clean_data = transform_exchange_data(data)
        write_to_motherduck_from_data_frame(clean_data)

        # Pipeline end time
        pipeline_end_time = datetime.datetime.now()

        # Pipeline execution time
<<<<<<< HEAD
        pipeline_execution_time = (
            pipeline_end_time - pipeline_start_time
        ).total_seconds()
        logger.info(f"Pipeline execution time: {pipeline_execution_time} secs")
=======
        pipeline_execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        logger.info(f"Pipeline execution time: {pipeline_execution_time} seconds")
>>>>>>> origin/ci

        # Log pipeline execution time evolution
        logger.info(f"Pipeline execution started at: {pipeline_start_time}")
        logger.info(f"Pipeline execution completed at: {pipeline_end_time}")

        # Calculate and log time per job
        job_times = []
        job_start_time = pipeline_start_time
        for job_name in ['extract', 'transform', 'load']:
            job_end_time = datetime.datetime.now()
            job_execution_time = (job_end_time - job_start_time).total_seconds()
            job_times.append(job_execution_time)
<<<<<<< HEAD
            logger.info(f"Time for {job_name} job: {job_execution_time} secs")
=======
            logger.info(f"Time for {job_name} job: {job_execution_time} seconds")
>>>>>>> origin/ci
            job_start_time = job_end_time

        # Calculate and log average pipeline execution time
        average_execution_time = sum(job_times) / len(job_times)
<<<<<<< HEAD
        logger.info(
            f"Avg pipeline execution time: {average_execution_time} secs"
        )
=======
        logger.info(f"Average pipeline execution time: {average_execution_time} seconds")
>>>>>>> origin/ci

    except Exception as e:
        logger.error(f"Error running pipeline: {e}")


if __name__ == '__main__':
    run_pipeline()
