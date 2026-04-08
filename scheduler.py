"""
APScheduler — 평일 15:40 자동 수집
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config import (
    SUPPLY_DEMAND_COLLECT_HOUR,
    SUPPLY_DEMAND_COLLECT_MINUTE,
    TIMEZONE,
)
from collector import collect_daily_supply_demand

logger = logging.getLogger(__name__)


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        collect_daily_supply_demand,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=SUPPLY_DEMAND_COLLECT_HOUR,
            minute=SUPPLY_DEMAND_COLLECT_MINUTE,
            timezone=TIMEZONE,
        ),
        id="supply_demand_daily",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    logger.info(
        "스케줄러 시작 — 매 영업일 %02d:%02d (KST) 수집",
        SUPPLY_DEMAND_COLLECT_HOUR,
        SUPPLY_DEMAND_COLLECT_MINUTE,
    )
    return scheduler
