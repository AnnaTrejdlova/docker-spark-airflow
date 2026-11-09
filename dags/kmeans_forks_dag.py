"""
KMeans pipeline showing how to build DAGs with forks (TaskFlow API)

  validate_data ─┬─ column_stats[x] ─┐
                 ├─ column_stats[y] ─┴──────────────────────────────────────────────┐
                 └─ check_spark ─ choose_engine ─┬─ run_kmeans_spark ─┐             │
                                                 └─ run_kmeans_local ─┴─ find_plot ─┴─ report ─ done

  (choose_engine also gets the row count from validate_data)

Patterns used:
  * fan-out / fan-in   one task feeds several parallel tasks, a later task waits for all of them
  * dynamic mapping    column_stats.expand() creates one task per column at run time
  * branching          choose_engine picks ONE of two paths, the other one is skipped
  * trigger rules      find_plot runs although one of its upstream tasks was skipped
  * task group         report folds two parallel tasks into one box in the UI
  * edge labels        Label() puts the branch condition on the arrow in the Graph view

Trigger it with "Trigger DAG w/ config" to pick the engine: auto (default), spark or local.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Label, Param, dag, get_current_context, task, task_group

DATA_FILE = '/opt/spark-data/artificial_data.csv'
COLUMNS = ['x', 'y']
# Below this many rows starting Spark is slower than clustering in plain Python
MIN_ROWS_FOR_SPARK = 10

SPARK_PLOT = '/opt/spark-data/output.png'  # written by apps/kmeans_clustering.py
LOCAL_PLOT = '/opt/spark-data/output_local.png'
SUMMARY_FILE = '/opt/spark-data/kmeans_summary.txt'
HISTORY_DIR = '/opt/spark-data/history'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='kmeans_forks_pipeline',
    default_args=default_args,
    description='KMeans pipeline with parallel tasks, branching and a task group',
    start_date=datetime(2026, 2, 16),
    schedule=None,  # trigger manually
    catchup=False,
    params={
        'engine': Param('auto', enum=['auto', 'spark', 'local'],
                        description='auto = Spark if the cluster is up and the data is big enough'),
    },
    tags=['example', 'spark', 'taskflow', 'branching'],
)
def kmeans_forks_pipeline():

    # --- 1. Start: one task -------------------------------------------------

    @task
    def validate_data():
        """Check the CSV and return its row count (the value goes to the tasks that use it)"""
        import pandas as pd

        if not os.path.isfile(DATA_FILE):
            raise FileNotFoundError(f"Data file {DATA_FILE} does not exist")
        df = pd.read_csv(DATA_FILE)
        missing = set(COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")
        if df.empty:
            raise ValueError("The data file has no rows")
        print(f"✓ {len(df)} rows in {DATA_FILE}")
        return len(df)

    # --- 2. Fan-out: these run in parallel after validate_data ---------------

    @task
    def column_stats(column: str):
        """Mapped task: Airflow runs one copy per column, see .expand() below"""
        import pandas as pd

        values = pd.read_csv(DATA_FILE)[column]
        return {
            'column': column,
            'min': float(values.min()),
            'max': float(values.max()),
            'mean': round(float(values.mean()), 2),
        }

    @task
    def check_spark():
        """Is the Spark master reachable? Decides between the Spark and the local path"""
        import socket

        try:
            with socket.create_connection(('spark-master', 7077), timeout=3):
                print("✓ Spark master is up")
                return True
        except OSError as error:
            print(f"✗ Spark master not reachable: {error}")
            return False

    # --- 3. Branch: returns the task_id of the path to run --------------------

    @task.branch
    def choose_engine(rows: int, spark_up: bool):
        engine = get_current_context()['params']['engine']
        if engine == 'auto':
            engine = 'spark' if spark_up and rows >= MIN_ROWS_FOR_SPARK else 'local'
        if engine == 'spark' and not spark_up:
            raise RuntimeError("engine=spark was requested but the Spark master is not reachable")
        print(f"Using engine: {engine} ({rows} rows, Spark up: {spark_up})")
        return 'run_kmeans_spark' if engine == 'spark' else 'run_kmeans_local'

    # --- 4. The two alternative paths -----------------------------------------

    run_kmeans_spark = SparkSubmitOperator(
        task_id='run_kmeans_spark',
        application='/opt/spark-apps/kmeans_clustering.py',
        verbose=True,
        conf={
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
        },
    )

    @task
    def run_kmeans_local():
        """Same clustering as kmeans_clustering.py, but with SciPy inside the Airflow container"""
        import matplotlib
        matplotlib.use('Agg')  # no display inside the container
        import matplotlib.pyplot as plt
        import pandas as pd
        from scipy.cluster.vq import kmeans2

        df = pd.read_csv(DATA_FILE)
        centers, labels = kmeans2(df[COLUMNS].to_numpy(dtype=float), 3, minit='++')

        plt.figure(figsize=(8, 6))
        for cluster_id in sorted(set(labels)):
            points = df[labels == cluster_id]
            plt.scatter(points['x'], points['y'], label=f"Cluster {cluster_id}")
        plt.scatter(centers[:, 0], centers[:, 1], color='black', marker='x', s=150, label='Centers')
        plt.title("KMeans Clustering (local, SciPy)")
        plt.xlabel("x")
        plt.ylabel("y")
        plt.legend()
        plt.savefig(LOCAL_PLOT)
        print(f"Výsledek byl uložen do: {LOCAL_PLOT}")

    # --- 5. Join after the branch ----------------------------------------------

    # The default trigger rule (all_success) would skip this task, because one of
    # its two upstream tasks is always skipped. This rule means: nothing failed
    # and at least one upstream task succeeded.
    @task(trigger_rule='none_failed_min_one_success')
    def find_plot():
        # Read the branch result from XCom instead of taking it as an argument:
        # an argument would make this a direct child of choose_engine, and a
        # branch task skips every direct child whose task_id it did not return
        chosen_task = get_current_context()['ti'].xcom_pull(task_ids='choose_engine')
        path = SPARK_PLOT if chosen_task == 'run_kmeans_spark' else LOCAL_PLOT
        if not os.path.isfile(path):
            raise FileNotFoundError(f"{chosen_task} did not create {path}")
        print(f"✓ Plot: {path}")
        return path

    # --- 6. Task group: two parallel tasks shown as one box --------------------

    @task
    def write_summary(stats: list, plot_path: str):
        """Fan-in: stats is the list of results from all column_stats copies"""
        lines = [f"KMeans run at {datetime.now():%Y-%m-%d %H:%M:%S}", f"Plot: {plot_path}", ""]
        for s in stats:
            lines.append(f"{s['column']}: min {s['min']}, max {s['max']}, mean {s['mean']}")
        with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        print('\n'.join(lines))

    @task
    def archive_plot(plot_path: str):
        """Keep a copy of every run's plot in data/history/"""
        import shutil

        os.makedirs(HISTORY_DIR, exist_ok=True)
        target = os.path.join(HISTORY_DIR, f"output_{datetime.now():%Y%m%d_%H%M%S}.png")
        shutil.copy(plot_path, target)
        print(f"✓ Copied to {target}")

    @task_group
    def report(stats: list, plot_path: str):
        # Both get the same inputs and do not depend on each other -> parallel
        write_summary(stats, plot_path)
        archive_plot(plot_path)

    @task.bash
    def done():
        return 'echo "Pipeline completed at $(date)"'

    # --- Wiring ------------------------------------------------------------------

    rows = validate_data()

    # Fan-out: one column_stats task per column, plus check_spark, all in parallel
    stats = column_stats.expand(column=COLUMNS)
    rows >> stats
    spark_up = check_spark()
    rows >> spark_up

    # Passing rows and spark_up as arguments makes choose_engine wait for both
    chosen = choose_engine(rows, spark_up)
    local_job = run_kmeans_local()
    chosen >> Label('Spark cluster') >> run_kmeans_spark
    chosen >> Label('plain Python') >> local_job

    # Join: find_plot waits for both paths, one of them is always skipped
    plot = find_plot()
    [run_kmeans_spark, local_job] >> plot

    # Fan-in: report waits for all column_stats copies and for find_plot
    report(stats, plot) >> done()


kmeans_forks_pipeline()
