import json
import pathlib
import csv
from datetime import datetime
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="download_rocket_local_csv_reports",
    description="Download rocket pictures with CSV reports",
    start_date=days_ago(14),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["rocket", "monitoring", "csv"],
)

start_task = DummyOperator(task_id="start_task", dag=dag)
end_task = DummyOperator(task_id="end_task", dag=dag)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming' || exit 1",
    dag=dag,
    retries=2,
)

def _get_pictures(**kwargs):
    images_dir = "/opt/airflow/data/images"
    report_data = []
    stats = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'start_time': datetime.now().isoformat()
    }
    
    pathlib.Path(images_dir).mkdir(parents=True, exist_ok=True)

    try:
        with open("/opt/airflow/data/launches.json") as f:
            launches = json.load(f)
            image_urls = [launch["image"] for launch in launches["results"] if launch.get("image")]
            stats['total'] = len(image_urls)
            
            for image_url in image_urls:
                record = {
                    'url': image_url,
                    'status': '',
                    'error': '',
                    'filename': '',
                    'timestamp': datetime.now().isoformat()
                }
                try:
                    response = requests.get(image_url, timeout=10)
                    if response.status_code == 200:
                        image_filename = image_url.split("/")[-1]
                        target_file = f"{images_dir}/{image_filename}"
                        with open(target_file, "wb") as f:
                            f.write(response.content)
                        record['status'] = 'success'
                        record['filename'] = image_filename
                        stats['success'] += 1
                    else:
                        record['status'] = 'failed'
                        record['error'] = f"HTTP {response.status_code}"
                        stats['failed'] += 1
                except requests_exceptions.RequestException as e:
                    record['status'] = 'failed'
                    record['error'] = str(e)
                    stats['failed'] += 1
                
                report_data.append(record)

        report_path = f"/opt/airflow/data/download_report_{datetime.now().date()}.csv"
        with open(report_path, 'w', newline='') as csvfile:
            fieldnames = ['url', 'status', 'error', 'filename', 'timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report_data)
        
        stats_path = f"/opt/airflow/data/stats_{datetime.now().date()}.csv"
        with open(stats_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=stats.keys())
            writer.writeheader()
            writer.writerow(stats)
            
        kwargs['ti'].xcom_push(key='download_stats', value=stats)
        
    except Exception as e:
        kwargs['ti'].xcom_push(key='error', value=str(e))
        raise

get_pictures = PythonOperator(
    task_id="get_pictures", 
    python_callable=_get_pictures, 
    dag=dag,
    provide_context=True,
)

def _generate_summary(**kwargs):
    ti = kwargs['ti']
    stats = ti.xcom_pull(task_ids='get_pictures', key='download_stats')
    
    summary_path = "/opt/airflow/data/summary_report.csv"
    file_exists = pathlib.Path(summary_path).exists()
    
    with open(summary_path, 'a', newline='') as csvfile:
        fieldnames = ['date', 'total', 'success', 'failed', 'start_time']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'date': datetime.now().date().isoformat(),
            'total': stats['total'],
            'success': stats['success'],
            'failed': stats['failed'],
            'start_time': stats['start_time']
        })

generate_summary = PythonOperator(
    task_id="generate_summary",
    python_callable=_generate_summary,
    dag=dag,
    provide_context=True,
)

start_task >> download_launches >> get_pictures >> generate_summary >> end_task
