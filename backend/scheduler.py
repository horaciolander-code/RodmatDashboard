"""
Daily Report Scheduler
Runs daily reports for all stores with reporting enabled.
Uses schedule library (lightweight, no daemon required).
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import schedule
from app.database import SessionLocal
from app.services.daily_report_service import run_all_reports


def daily_job():
    print(f"Running daily reports...")
    db = SessionLocal()
    try:
        run_all_reports(db)
    except Exception as e:
        print(f"Error in daily reports: {e}")
    finally:
        db.close()
    print("Daily reports complete.")


if __name__ == "__main__":
    # Default: run at 8:00 AM daily
    schedule.every().day.at("08:00").do(daily_job)
    print("Scheduler started. Daily reports at 08:00.")
    print("Press Ctrl+C to stop.")

    while True:
        schedule.run_pending()
        time.sleep(60)
