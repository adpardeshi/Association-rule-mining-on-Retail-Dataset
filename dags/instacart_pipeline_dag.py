from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# DEFAULT ARGUMENTS
# ============================================================

default_args = {
    "owner": "aniket",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="instacart_association_rule_pipeline",

    description=(
        "Instacart Big Data Pipeline using "
        "Apache Spark, FP-Growth and PostgreSQL"
    ),

    default_args=default_args,

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "instacart",
        "big-data",
        "spark",
        "fp-growth",
        "postgresql",
    ],
) as dag:

    # ========================================================
    # TASK 1
    # SPARK PREPROCESSING
    # ========================================================

    preprocess = BashOperator(
        task_id="spark_preprocessing",

        bash_command="""
        echo "=============================================="
        echo "TASK 1: SPARK PREPROCESSING"
        echo "=============================================="

        echo "Running: /opt/instacart/spark/preprocess.py"

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/instacart/spark/preprocess.py

        echo "=============================================="
        echo "PREPROCESSING COMPLETED"
        echo "=============================================="
        """,
    )


    # ========================================================
    # TASK 2
    # FP-GROWTH
    # ========================================================

    fp_growth = BashOperator(
        task_id="spark_fp_growth",

        bash_command="""
        echo "=============================================="
        echo "TASK 2: FP-GROWTH ASSOCIATION RULE MINING"
        echo "=============================================="

        echo "Running: /opt/instacart/spark/fp_growth.py"

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/instacart/spark/fp_growth.py

        echo "=============================================="
        echo "FP-GROWTH COMPLETED"
        echo "=============================================="
        """,
    )


    # ========================================================
    # TASK 3
    # LOAD TO POSTGRESQL
    # ========================================================

    load_postgres = BashOperator(
        task_id="load_rules_to_postgresql",

        bash_command="""
        echo "=============================================="
        echo "TASK 3: LOADING RULES TO POSTGRESQL"
        echo "=============================================="

        echo "Running: /opt/instacart/spark/load_to_postgres.py"

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/instacart/spark/load_to_postgres.py

        echo "=============================================="
        echo "POSTGRESQL LOAD COMPLETED"
        echo "=============================================="
        """,
    )


    # ========================================================
    # PIPELINE DEPENDENCIES
    # ========================================================

    preprocess >> fp_growth >> load_postgres