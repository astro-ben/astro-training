from airflow import DAG  ## by default, this is needed for the dagbag to parse this file
import dagfactory
from pathlib import Path

config_file = Path.cwd() / "include/config_file.yml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())