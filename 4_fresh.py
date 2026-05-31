import sys, os
from datetime import datetime
from sqlalchemy import create_engine, text
import requests



class FreshnessMonitor:
    def __init__(self, database_url, threshold_hours=2):
        self.database_url = database_url
        self.threshold_sec = threshold_hours * 3600
        self.engine = None
    
    def _log_alert(self, msg):
        with open('alerts.log', 'a') as f:
            f.write(f"{datetime.now()} - {msg}\n")
        if token := "TG_TOKEN":
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                         json={'chat_id': "TG_CHAT_ID", 'text': msg}, timeout=10)
    
    def run_check(self):
        try:
            self.engine = create_engine(self.database_url)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))  
                max_date = conn.execute(text("SELECT MAX(created_at) FROM orders")).scalar()
                
            if not max_date:
                self._write_monitoring('ALERT', None, "Таблица пуста")
                self._log_alert("⚠️ ALERT: Таблица orders пуста")
                return False
                
            lag_sec = (datetime.now() - max_date).total_seconds()
            status, details = ('ALERT', f"Отставание {lag_sec/3600:.1f}ч > {self.threshold_sec/3600}ч") if lag_sec > self.threshold_sec else ('OK', "OK")
            
            if status == 'ALERT':
                self._log_alert(f"⚠️ ALERT: orders | Последняя запись: {max_date} | Отставание: {lag_sec/3600:.1f}ч")
            
            self._write_monitoring(status, lag_sec/3600, details)
            return status == 'OK'
            
        except Exception as e:
            self._write_monitoring('ERROR', None, str(e))
            return False
        finally:
            if self.engine: self.engine.dispose()
    
    def _write_monitoring(self, status, metric_hours, details):
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dq_monitoring (
                    id SERIAL PRIMARY KEY, 
                    table_name VARCHAR(100), 
                    check_type VARCHAR(50),
                    check_time TIMESTAMP DEFAULT NOW(), 
                    metric_value NUMERIC, 
                    threshold NUMERIC,
                    status VARCHAR(10), 
                    details TEXT
                )
            """))
            conn.execute(text("""
                INSERT INTO dq_monitoring (table_name, check_type, metric_value, threshold, status, details)
                VALUES ('orders', 'freshness_check', :metric, :threshold, :status, :details)
            """), {'metric': metric_hours, 'threshold': self.threshold_sec/3600, 'status': status, 'details': details})
            conn.commit()

def main():
    monitor = FreshnessMonitor(
        os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@127.0.0.1:5432/pg_week8'),
        threshold_hours=int(os.getenv('FRESHNESS_THRESHOLD_HOURS', 2))
    )
    sys.exit(0 if monitor.run_check() else 1)

if __name__ == "__main__":
    main()