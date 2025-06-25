"""
Scheduler for RSS collection jobs.
Handles periodic execution of RSS collection tasks.
"""
import logging
import schedule
import time
import threading
from datetime import datetime
from typing import List, Callable, Optional
import pytz

logger = logging.getLogger(__name__)

class FeedScheduler:
    """
    Manages scheduled execution of RSS collection tasks.
    """
    
    def __init__(self, times: List[str], timezone: str = "Asia/Kolkata"):
        """
        Initialize the scheduler.
        
        Args:
            times: List of times in HH:MM format (e.g., ["05:00", "14:00"])
            timezone: Timezone for scheduling (default: Asia/Kolkata)
        """
        self.times = times
        self.timezone = timezone
        self.running = False
        self.thread = None
        self.job_function = None
        
        # Setup timezone
        try:
            self.tz = pytz.timezone(timezone)
            logger.info(f"Scheduler initialized with timezone: {timezone}")
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warning(f"Unknown timezone: {timezone}, using system default")
            self.tz = None
        
        logger.debug(f"FeedScheduler initialized for times: {times}")
    
    def add_collection_job(self, job_function: Callable[[], None]):
        """
        Add a collection job to be executed at scheduled times.
        
        Args:
            job_function: Function to execute for collection
        """
        self.job_function = job_function
        
        # Clear any existing jobs
        schedule.clear()
        
        # Schedule jobs for each specified time
        for time_str in self.times:
            schedule.every().day.at(time_str).do(self._run_job_safely)
            logger.info(f"Scheduled collection job at {time_str}")
        
        logger.info(f"Added collection job with {len(self.times)} scheduled times")
    
    def _run_job_safely(self):
        """
        Run the job function with error handling.
        """
        if not self.job_function:
            logger.error("No job function defined")
            return
        
        try:
            logger.info("Executing scheduled collection job...")
            start_time = time.time()
            
            # Execute the job
            result = self.job_function()
            
            duration = time.time() - start_time
            logger.info(f"Scheduled job completed in {duration:.2f} seconds")
            
            # Log job results if available
            if isinstance(result, dict):
                total_articles = result.get('total_new_articles', 0)
                total_keywords = result.get('total_keywords', 0)
                logger.info(f"Job results: {total_articles} new articles from {total_keywords} keywords")
            
        except Exception as e:
            logger.error(f"Error executing scheduled job: {e}", exc_info=True)
    
    def start(self):
        """
        Start the scheduler in a separate thread.
        """
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        
        # Start scheduler thread
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        
        logger.info("Scheduler started")
    
    def stop(self):
        """
        Stop the scheduler.
        """
        if not self.running:
            logger.warning("Scheduler is not running")
            return
        
        self.running = False
        
        # Wait for thread to finish
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # Clear scheduled jobs
        schedule.clear()
        
        logger.info("Scheduler stopped")
    
    def _scheduler_loop(self):
        """
        Main scheduler loop running in separate thread.
        """
        logger.debug("Scheduler loop started")
        
        while self.running:
            try:
                # Run pending jobs
                schedule.run_pending()
                
                # Sleep for a short period
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                time.sleep(5)  # Wait longer after error
        
        logger.debug("Scheduler loop stopped")
    
    def get_next_run_time(self) -> Optional[datetime]:
        """
        Get the next scheduled run time.
        
        Returns:
            Next run time as datetime or None if no jobs scheduled
        """
        try:
            jobs = schedule.get_jobs()
            if not jobs:
                return None
            
            # Get the next run time from all jobs
            next_runs = [job.next_run for job in jobs if job.next_run]
            
            if next_runs:
                next_run = min(next_runs)
                
                # Convert to timezone-aware datetime if timezone is set
                if self.tz:
                    # schedule library uses naive datetime, assume local time
                    local_dt = self.tz.localize(next_run)
                    return local_dt
                else:
                    return next_run
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting next run time: {e}")
            return None
    
    def get_status(self) -> dict:
        """
        Get scheduler status information.
        
        Returns:
            Dictionary with scheduler status
        """
        try:
            next_run = self.get_next_run_time()
            
            status = {
                "running": self.running,
                "scheduled_times": self.times,
                "timezone": self.timezone,
                "next_run": next_run.isoformat() if next_run else None,
                "jobs_count": len(schedule.get_jobs()),
                "thread_alive": self.thread.is_alive() if self.thread else False
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {"error": str(e)}
    
    def run_now(self):
        """
        Execute the collection job immediately (outside of schedule).
        """
        if not self.job_function:
            logger.error("No job function defined")
            return
        
        logger.info("Running collection job immediately...")
        self._run_job_safely()