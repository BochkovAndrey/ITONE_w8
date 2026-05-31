# dags/data_quality_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import pandas as pd
import logging
from sqlalchemy import create_engine, text
import os

import sys
sys.path.append('/opt/airflow/dags/scripts')
from 4_fresh import FreshnessMonitor

default_args = {
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def load_data_from_csv(**context):
    
    DATABASE_URL = Variable.get('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/pg_week8')
    CSV_PATH = Variable.get('CSV_FILE_PATH', '/opt/airflow/data/orders.csv')
    
    try:
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV файл не найден по пути: {CSV_PATH}")

        df = pd.read_csv(CSV_PATH, encoding='utf-8')
        
        engine = create_engine(DATABASE_URL)
        
        # Очистка таблицы перед загрузкой
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE orders RESTART IDENTITY"))
            conn.commit()
        
        columns_to_insert = ['order_id', 'customer_id', 'amount', 'currency','status', 'payment_method', 'created_at','updated_at', 'shipped_at', 'region']
              
        df[columns_to_insert].to_sql('orders', engine, if_exists='append', index=False, chunksize=1000)
        
        rows_loaded = len(df)
        context['task_instance'].xcom_push(key='rows_loaded', value=rows_loaded)
        logging.info(f"Загружено {rows_loaded} записей")
        
        return True
        
    except FileNotFoundError as e:
        logging.error(f"Ошибка: {e}")
        raise
    except pd.errors.EmptyDataError:
        logging.error("CSV файл пуст")
        raise
    except pd.errors.ParserError as e:
        logging.error(f"Ошибка парсинга CSV: {e}")
        raise
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных: {e}")
        raise

def write_dq_result_to_monitoring(engine, check_name, metric_value, threshold, status, details):
    try:
        with engine.connect() as conn:
            insert_query = text("""
                INSERT INTO dq_monitoring 
                (table_name, check_type, check_time, metric_value, threshold, status, details)
                VALUES 
                (:table_name, :check_type, :check_time, :metric_value, :threshold, :status, :details)
            """)
            
            conn.execute(insert_query, {
                'table_name': 'orders',
                'check_type': check_name,
                'check_time': datetime.now(),
                'metric_value': metric_value,
                'threshold': threshold,
                'status': status,
                'details': details
            })
            conn.commit()
        logging.info(f"Результат DQ проверки '{check_name}' записан в dq_monitoring: статус={status}")
    except Exception as e:
        logging.error(f"Ошибка записи в dq_monitoring: {e}")

def run_quality_checks(**context):
    
    import great_expectations as gx
    from datetime import datetime
    
    DATABASE_URL = Variable.get('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/pg_week8')
    engine = create_engine(DATABASE_URL)
    
    try:
        gx_context = gx.get_context()

        data_source = gx_context.data_sources.add_sql(
            name="pg_week8",
            connection_string=DATABASE_URL
        )
        
        data_asset = data_source.add_table_asset(name="orders", table_name="orders")

        suite = gx_context.suites.add(gx.ExpectationSuite(name="orders_quality_suite"))
        
        # Добавление ожиданий
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0, max_value=200000
        ))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
            column="status", value_set=["new", "paid", "shipped", "cancelled"]
        ))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
            column="created_at", max_value=datetime.now()
        ))
        
        batch_request = data_asset.build_batch_request()
        validator = gx_context.get_validator(batch_request=batch_request, expectation_suite=suite)
        results = validator.validate()
        
        gx_context.build_data_docs()
        
        errors_found = []
        total_violations = 0
        
        for expectation in suite.expectations:
            for res in results.results:
                if res.expectation_config['type'] == expectation.type:
                    check_name = expectation.type
                    unexpected_count = res.result.get('unexpected_count', 0) if not res.success else 0
                    total_violations += unexpected_count
                    
                    check_status = 'OK' if res.success else 'ALERT'
                    
                    column = expectation.kwargs.get('column', 'N/A')
                    details = f"Колонка: {column}, нарушений: {unexpected_count}"
                    if not res.success and 'partial_unexpected_list' in res.result:
                        details += f", примеры: {res.result['partial_unexpected_list'][:3]}"
                    
                    write_dq_result_to_monitoring(
                        engine, 
                        check_name, 
                        unexpected_count, 
                        0,  
                        check_status,
                        details
                    )
                    
                    if not res.success:
                        errors_found.append(f"  - {check_name}: {unexpected_count} нарушений")
                        if 'partial_unexpected_list' in res.result:
                            errors_found.append(f"    Примеры: {res.result['partial_unexpected_list'][:3]}")
                    break
        
        overall_status = 'OK' if results.success else 'ALERT'
        overall_details = f"Всего проверок: {len(suite.expectations)}, успешно: {results.statistics['successful_expectations']}, провалено: {results.statistics['unsuccessful_expectations']}, всего нарушений: {total_violations}"
        
        write_dq_result_to_monitoring(
            engine,
            'overall_quality_check',
            total_violations, 
            0, 
            overall_status,
            overall_details
        )
        
        context['task_instance'].xcom_push(key='quality_success', value=results.success)
        context['task_instance'].xcom_push(key='quality_details', value='\n'.join(errors_found) if errors_found else "Все проверки пройдены успешно")
        
        logging.info(f"Результаты проверки качества: {'УСПЕШНО' if results.success else 'ПРОВАЛ'}")
        logging.info(f"Записано {len(suite.expectations) + 1} записей в dq_monitoring")
        if errors_found:
            logging.warning("\n".join(errors_found))
        
        return results.success
        
    except Exception as e:
        logging.error(f"Ошибка при проверке качества: {e}")
        raise
    finally:
        engine.dispose()

def run_freshness_monitoring(**context):
    
    DATABASE_URL = Variable.get('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/pg_week8')
    THRESHOLD_HOURS = int(Variable.get('FRESHNESS_THRESHOLD_HOURS', default_var=2))
    
    try:
        monitor = FreshnessMonitor(DATABASE_URL, threshold_hours=THRESHOLD_HOURS)
        success, status, lag_seconds = monitor.run_check()
        
        context['task_instance'].xcom_push(key='freshness_success', value=success)
        context['task_instance'].xcom_push(key='freshness_status', value=status)
        context['task_instance'].xcom_push(key='freshness_lag', value=lag_seconds)
        
        if not success:
            error_msg = f"Мониторинг свежести провален: статус={status}, отставание={lag_seconds} сек"
            logging.error(error_msg)
            raise Exception(error_msg)
        
        logging.info(f"Мониторинг свежести успешен: статус={status}, отставание={lag_seconds} сек")
        return success
        
    except Exception as e:
        logging.error(f"Ошибка в мониторинге свежести: {e}")
        raise

def generate_report(**context):
    rows_loaded = context['task_instance'].xcom_pull(task_ids='load_data', key='rows_loaded')
    quality_success = context['task_instance'].xcom_pull(task_ids='quality_check', key='quality_success')
    quality_details = context['task_instance'].xcom_pull(task_ids='quality_check', key='quality_details')
    freshness_success = context['task_instance'].xcom_pull(task_ids='freshness_monitoring', key='freshness_success')
    freshness_status = context['task_instance'].xcom_pull(task_ids='freshness_monitoring', key='freshness_status')
    freshness_lag = context['task_instance'].xcom_pull(task_ids='freshness_monitoring', key='freshness_lag')
    
    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append("ОТЧЕТ О КАЧЕСТВЕ ДАННЫХ")
    report_lines.append(f"Время генерации: {datetime.now()}")
    report_lines.append("=" * 60)
    
    if rows_loaded:
        report_lines.append(f"\n📊 ЗАГРУЗКА ДАННЫХ:")
        report_lines.append(f"   Загружено записей: {rows_loaded}")
    
    report_lines.append(f"\n🔍 ПРОВЕРКА КАЧЕСТВА (Great Expectations):")
    report_lines.append(f"   Статус: {'✅ УСПЕШНО' if quality_success else '❌ ПРОВАЛ'}")
    if quality_details:
        report_lines.append(f"   Детали:\n{quality_details}")
    
    report_lines.append(f"\n⏰ МОНИТОРИНГ СВЕЖЕСТИ:")
    report_lines.append(f"   Статус: {'✅ УСПЕШНО' if freshness_success else '❌ ПРОВАЛ'}")
    report_lines.append(f"   Статус проверки: {freshness_status}")
    if freshness_lag:
        report_lines.append(f"   Отставание: {freshness_lag:.0f} сек ({freshness_lag/3600:.2f} час)")
    
    report_lines.append("\n" + "=" * 60)
    report_lines.append("ИТОГОВЫЙ СТАТУС:")
    if quality_success and freshness_success:
        report_lines.append("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО")
    else:
        report_lines.append("❌ ОБНАРУЖЕНЫ ПРОБЛЕМЫ С КАЧЕСТВОМ ДАННЫХ")
    report_lines.append("=" * 60)
    
    report_text = "\n".join(report_lines)
    
    report_dir = '/opt/airflow/logs/reports'
    os.makedirs(report_dir, exist_ok=True)
    report_path = f"{report_dir}/quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    logging.info(f"Отчет сохранен: {report_path}")
    
    context['task_instance'].xcom_push(key='report_path', value=report_path)
    context['task_instance'].xcom_push(key='report_text', value=report_text)
    
    return report_path

def send_notification(**context):
    
    report_path = context['task_instance'].xcom_pull(task_ids='generate_report', key='report_path')
    quality_success = context['task_instance'].xcom_pull(task_ids='quality_check', key='quality_success')
    freshness_success = context['task_instance'].xcom_pull(task_ids='freshness_monitoring', key='freshness_success')
    

    telegram_token = Variable.get('TELEGRAM_BOT_TOKEN', default_var=None)
    telegram_chat_id = Variable.get('TELEGRAM_CHAT_ID', default_var=None)
    
    if telegram_token and telegram_chat_id:
        try:
            import requests
            message = f"📊 *Отчет о качестве данных*\n\n"
            message += f"Качество данных: {'✅ Успешно' if quality_success else '❌ Провал'}\n"
            message += f"Свежесть данных: {'✅ Успешно' if freshness_success else '❌ Провал'}\n"
            message += f"Отчет сохранен: {report_path}"
            
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            payload = {
                'chat_id': telegram_chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logging.info("Уведомление отправлено в Telegram")
            else:
                logging.warning(f"Не удалось отправить в Telegram: {response.text}")
        except Exception as e:
            logging.warning(f"Ошибка отправки в Telegram: {e}")
    else:
        logging.info("Telegram не настроен, пропускаем отправку уведомления")
    
    return True



dag = DAG(
    'data_quality_pipeline',
    default_args=default_args,
    description='DAG для контроля качества данных заказов',
    schedule_interval='0 */6 * * *',
    catchup=False,
    tags=['data_quality', 'orders', 'monitoring'],
)

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS orders (
            order_id      BIGINT,
            customer_id   BIGINT,
            amount        NUMERIC(12, 2),
            currency      VARCHAR(3) DEFAULT 'RUB',
            status        VARCHAR(20),
            payment_method VARCHAR(30),
            created_at    TIMESTAMP,
            updated_at    TIMESTAMP,
            shipped_at    TIMESTAMP,
            region        VARCHAR(50)
        );
        
        CREATE TABLE IF NOT EXISTS dq_monitoring (
            check_id      SERIAL PRIMARY KEY,
            table_name    VARCHAR(100),
            check_type    VARCHAR(50),
            check_time    TIMESTAMP DEFAULT NOW(),
            metric_value  NUMERIC,
            threshold     NUMERIC,
            status        VARCHAR(10),
            details       TEXT
        );
    """,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_from_csv,
    dag=dag,
)

quality_check = PythonOperator(
    task_id='quality_check',
    python_callable=run_quality_checks,
    dag=dag,
)

freshness_monitoring = PythonOperator(
    task_id='freshness_monitoring',
    python_callable=run_freshness_monitoring,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

send_notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag,
)

create_tables >> load_data >> [quality_check, freshness_monitoring] >> generate_report_task >> send_notification_task