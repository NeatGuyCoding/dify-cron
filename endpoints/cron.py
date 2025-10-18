import datetime
# Configure logging
import json
import logging
import threading
import time
from collections.abc import Mapping
from typing import Dict
from zoneinfo import ZoneInfo

import requests
from dify_plugin import Endpoint
from dify_plugin.core.runtime import Session
from werkzeug import Request, Response
class DifyPluginLogHandler(logging.Handler):
    """Custom log handler that outputs logs in dify-plugin-daemon format"""
    
    def emit(self, record):
        try:
            # Format the log message
            log_entry = {
                "session_id": "",
                "event": "log",
                "data": {
                    "level": record.levelname.lower(),
                    "message": self.format(record),
                    "timestamp": time.time()
                }
            }
            # Output as JSON to stdout
            print(json.dumps(log_entry), flush=True)
        except Exception:
            self.handleError(record)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        DifyPluginLogHandler()
    ]
)
logger = logging.getLogger(__name__)


class CronTaskManager:
    """Manage cron task execution with independent threads"""
    
    def __init__(self):
        self._running_tasks: Dict[str, threading.Thread] = {}
        self._lock = threading.Lock()
    
    def start_cron_task(self, app_id: str, session: Session, cron) -> bool:
        """Start cron task for specified app with independent thread"""
        with self._lock:
            if app_id in self._running_tasks:
                logger.warning(f"Cron task already running for app {app_id}")
                return False
            
            # Create independent thread for this cron loop
            thread = threading.Thread(
                target=self._run_cron_loop,
                args=(app_id, session, cron),
                name=f"cron-{app_id}",
                daemon=True  # Daemon thread will be killed when main process exits
            )
            thread.start()
            self._running_tasks[app_id] = thread
            logger.info(f"Started cron task for app {app_id} in independent thread")
            return True
    
    def stop_cron_task(self, app_id: str) -> bool:
        """Stop cron task for specified app"""
        with self._lock:
            if app_id not in self._running_tasks:
                logger.warning(f"No running cron task found for app {app_id}")
                return False
            
            # Get the thread and mark it for stopping
            thread = self._running_tasks[app_id]
            del self._running_tasks[app_id]
            logger.info(f"Stopped cron task for app {app_id}")
            
            # Note: We don't forcefully kill the thread here as it's a daemon thread
            # The thread will naturally exit when the cron loop detects the task is stopped
            return True
    
    def is_task_running(self, app_id: str) -> bool:
        """Check if cron task is running for specified app"""
        with self._lock:
            return app_id in self._running_tasks
    
    def get_running_tasks(self) -> Dict[str, threading.Thread]:
        """Get all running tasks"""
        with self._lock:
            return self._running_tasks.copy()
    
    def get_task_count(self) -> int:
        """Get number of running tasks"""
        with self._lock:
            return len(self._running_tasks)
    
    def _run_cron_loop(self, app_id: str, session: Session, cron) -> None:
        """Execute cron loop in independent thread"""
        logger.info(f"Starting cron loop for app {app_id} with cron string: {cron.cron_str}")
        
        try:
            cron_loop(session, app_id, cron)
        except Exception as e:
            logger.error(f"Fatal error in cron loop for app {app_id}: {str(e)}")
        finally:
            # Clean up task record
            with self._lock:
                self._running_tasks.pop(app_id, None)
            logger.info(f"Cron loop ended for app {app_id}")


# Global task manager instance
_cron_task_manager = CronTaskManager()


def cleanup_on_shutdown():
    """Clean up all tasks when application shuts down"""
    logger.info("Shutting down cron task manager...")
    running_tasks = _cron_task_manager.get_running_tasks()
    
    for app_id in list(running_tasks.keys()):
        logger.info(f"Stopping cron task for app {app_id}")
        _cron_task_manager.stop_cron_task(app_id)
    
    # Wait for all daemon threads to finish (they should exit quickly)
    logger.info("Waiting for cron threads to finish...")
    for thread in running_tasks.values():
        if thread.is_alive():
            thread.join(timeout=5)  # Wait up to 5 seconds for each thread
    
    logger.info("Cron task manager shutdown complete")


# Register cleanup function
import atexit

atexit.register(cleanup_on_shutdown)

STATUS_ACTIVE_HTML = """
<html><head></head><body>app-id: {app-id}<br>
Now@UTC: {now_utc}<br>
Now@{timezone}: {now}<br>
seconds,minutes,hours,days,months,weekdays: {s},{m},{h},{d},{months},{w}<br>
Cron status: active <br><a href='./stop'>Stop?</a></body></html>
"""
STATUS_INACTIVE_HTML = """
<html><head></head><body>app-id: {app-id}<br>
Now@UTC: {now_utc}<br>
Now@{timezone}: {now}<br>
seconds,minutes,hours,days,months,weekdays: {s},{m},{h},{d},{months},{w}<br>
Cron status: inactive <br><a href='./start'>Start?</a></body></html>
"""

STOP_HTML = '<html><head></head><body>Stop requested. Returning in 5 seconds... <meta http-equiv="refresh" content="5;URL=./status"></body></html>'
START_HTML = (
    '<html><head></head><body>Cron started. Returning in 5 seconds... <meta http-equiv="refresh" content="5;URL=./status"></body></html>',
)
ALREADY_STARTED_HTML = '<html><head></head><body>Cron already started. Returning in 5 seconds... <meta http-equiv="refresh" content="5;URL=./status"></body></html>'
ALREADY_STOPPED_HTML = (
    "<html><head></head><body>Cron was not running. Returning in 5 seconds... "
    '<meta http-equiv="refresh" content="5;URL=./status"></body></html>'
)


class Cron:
    def __init__(self, cron_str: str, timezone: str = "UTC"):
        self.cron_str = cron_str
        if len(cron_str.split(" ")) != 6:
            raise Exception("Invalid cron setting")
        self.timezone = timezone
        self.seconds, self.minutes, self.hours, self.days, self.months, self.weekdays = cron_str.split(" ")
        self.schedule = self.calc_schedule()

    def calc(self, arg: str, min: int, max: int) -> list[int]:
        # Calculate exact values for the argument in most cases.
        # For example, if arg = "*/15" and it is "minutes", then it should be min=0,max=59 and return [0,15,30,45].
        if arg == "*":
            return [-1]
        if arg.startswith("*/"):
            step = int(arg[2:])
            return [x for x in range(min, max + 1) if x % step == 0]

        li = [x for x in map(int, arg.split(",")) if min <= x and x <= max]
        return li

    def calc_schedule(self) -> dict:
        # Calculate schedule data compatible to https://docs.cron-job.org/rest-api.html#jobschedule
        return {
            "timezone": self.timezone,
            "seconds": self.calc(self.seconds, 0, 59),
            "hours": self.calc(self.hours, 0, 23),
            "minutes": self.calc(self.minutes, 0, 59),
            "mdays": self.calc(self.days, 1, 31),
            "months": self.calc(self.months, 1, 12),
            "wdays": self.calc(self.weekdays, 0, 6),
        }

    # def _match(self, field: str, current: int, allow_step: bool = False) -> bool:
    #    """Return True if the cron field matches the current value."""
    #    if field == "*":
    #        return True
    #    if field.startswith("*/"):
    #        if not allow_step:
    #            raise Exception("Invalid cron setting")
    #        try:
    #            step = int(field[2:])
    #        except ValueError as exc:
    #            raise Exception("Invalid cron setting") from exc
    #        if step <= 0:
    #            raise Exception("Invalid cron setting")
    #        return current % step == 0
    #    try:
    #        return current in map(int, field.split(","))
    #    except ValueError as exc:
    #        raise Exception("Invalid cron setting") from exc

    def is_now_to_call(self):
        # Check if it is time to make a self call
        # This cron is mostly based on UNIX cron format: https://www.ibm.com/docs/en/db2-as-a-service?topic=task-unix-cron-format
        # Sunday = 0, Monday = 1, ... and lastly Saturday = 6
        # This cron also supports seconds and step values (e.g. */5)
        now = datetime.datetime.now(tz=ZoneInfo(self.timezone))

        if not (self.seconds == "*" or now.second in self.schedule["seconds"]):
            return False
        if not (self.minutes == "*" or now.minute in self.schedule["minutes"]):
            return False
        if not (self.hours == "*" or now.hour in self.schedule["hours"]):
            return False
        if not (self.days == "*" or now.day in self.schedule["mdays"]):
            return False
        if not (self.months == "*" or now.month in self.schedule["months"]):
            return False
        if not (self.weekdays == "*" or now.weekday() in self.schedule["wdays"]):
            return False
        return True


def run_once(session: Session, app_id: str):
    try:
        logger.info(f"Executing cron job for app {app_id}")
        session.app.chat.invoke(app_id, "!cron!", {"is_cron": "yes"}, "blocking", "")
        logger.info(f"Cron job completed successfully for app {app_id}")
    except Exception as e:
        logger.error(f"Failed to execute cron job for app {app_id}: {str(e)}")
        raise


def cron_loop(session: Session, app_id: str, cron: Cron) -> None:
    is_triggered = False
    loop_count = 0
    logger.info(f"Starting cron loop for app {app_id} with cron string: {cron.cron_str}")

    try:
        while True:
            # Check if task is still running (managed by CronTaskManager)
            if not _cron_task_manager.is_task_running(app_id):
                logger.info(f"Cron loop stopped for app {app_id} - task manager reports not running")
                break

            loop_count += 1
            if loop_count % 1000 == 0:  # Log every 1000 loops
                logger.info(f"Cron loop running for app {app_id}, loop count: {loop_count}")

            time.sleep(0.1)

            try:
                if cron.is_now_to_call():
                    if not is_triggered:
                        logger.info(f"Cron trigger detected for app {app_id}")
                        run_once(session, app_id)
                        is_triggered = True
                else:
                    is_triggered = False
            except Exception as e:
                logger.error(f"Error in cron loop for app {app_id}: {str(e)}")
                time.sleep(1)
                
    except Exception as e:
        logger.error(f"Fatal error in cron loop for app {app_id}: {str(e)}")
        raise
    finally:
        logger.info(f"Cron loop ended for app {app_id}")


class CronJobAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_jobs(self) -> list[dict]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        result = requests.get(
            "https://api.cron-job.org/jobs",
            headers=headers,
        )
        return result.json()["jobs"]

    def get_job_ids(self) -> list[int]:
        jobs = self.get_jobs()
        return [job["jobId"] for job in jobs]

    def get_job_urls(self) -> list[str]:
        jobs = self.get_jobs()
        return [job["url"] for job in jobs]

    def register_job(self, job: dict):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        result = requests.put("https://api.cron-job.org/jobs", headers=headers, data=json.dumps(job))
        return result.json()["jobId"]

    def delete_job(self, job_id: int) -> None:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        result = requests.delete(f"https://api.cron-job.org/jobs/{job_id}", headers=headers)

    def delete_job_by_url(self, url: str) -> None:
        for job in self.get_jobs():
            if job["url"] == url:
                self.delete_job(job["jobId"])

    def register_dify_job(self, url: str, cron: Cron):
        # Reference: https://docs.cron-job.org/rest-api.html
        schedule = cron.calc_schedule()
        schedule["expiresAt"] = 0
        job = {
            "job": {
                "url": url,
                "enabled": True,
                "saveResponses": True,
                "schedule": schedule,
                "requestMethod": 0,  # GET
            }
        }
        return self.register_job(job)


class CronEndpoint(Endpoint):
    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        """
        Invokes the endpoint with the given request.
        """
        if settings.get("server_type") == "cloud":
            return self.run_cloud(r, values, settings)
        else:
            return self.run_local(r, values, settings)

    def run_local(self, r: Request, values: Mapping, settings: Mapping) -> Request:
        command = values["command"]
        app_id = settings.get("app")["app_id"]
        cron = Cron(settings.get("cron_str"), timezone=settings.get("timezone", time.tzname[0]))

        try:
            cron.is_now_to_call()
        except:
            raise Exception("Invalid cron setting")

        if len(command) == 0 or command == "status":
            # Check if task is running
            is_running = _cron_task_manager.is_task_running(app_id)
            if is_running:
                html = STATUS_ACTIVE_HTML
            else:
                html = STATUS_INACTIVE_HTML

            # Replace status page variables
            html = html.replace("{app-id}", app_id)
            html = html.replace("{now_utc}", datetime.datetime.now(tz=ZoneInfo("UTC")).strftime("%d/%m/%Y, %H:%M:%S"))
            html = html.replace(
                "{now}", datetime.datetime.now(tz=ZoneInfo(cron.timezone)).strftime("%d/%m/%Y, %H:%M:%S")
            )
            html = html.replace("{timezone}", cron.timezone)
            html = html.replace("{s}", str(cron.schedule["seconds"]))
            html = html.replace("{m}", str(cron.schedule["minutes"]))
            html = html.replace("{h}", str(cron.schedule["hours"]))
            html = html.replace("{d}", str(cron.schedule["mdays"]))
            html = html.replace("{months}", str(cron.schedule["months"]))
            html = html.replace("{w}", str(cron.schedule["wdays"]))

            # Add running tasks information
            running_tasks = _cron_task_manager.get_running_tasks()
            tasks_info = f"<br>Running tasks: {len(running_tasks)}<br>Task IDs: {list(running_tasks.keys())}"
            html = html.replace("</body>", f"{tasks_info}</body>")

            return Response(
                html,
                status=200,
                content_type="text/html",
            )

        elif command == "stop":
            success = _cron_task_manager.stop_cron_task(app_id)
            if success:
                return Response(
                    STOP_HTML,
                    status=200,
                    content_type="text/html",
                )
            else:
                return Response(
                    ALREADY_STOPPED_HTML,
                    status=200,
                    content_type="text/html",
                )

        elif command == "start":
            success = _cron_task_manager.start_cron_task(app_id, self.session, cron)
            if success:
                return Response(
                    START_HTML,
                    status=200,
                    content_type="text/html",
                )
            else:
                return Response(
                    ALREADY_STARTED_HTML,
                    status=200,
                    content_type="text/html",
                )
        else:
            return Response("Invalid Command")

    def run_cloud(self, r: Request, values: Mapping, settings: Mapping):
        if "cron_job_org_key" not in settings:
            raise Exception("Please input an API Key from https://cron-job.org")
        api = CronJobAPI(settings["cron_job_org_key"])
        command = values["command"]
        app_id = settings.get("app")["app_id"]
        cron = Cron(settings.get("cron_str"), timezone=settings.get("timezone", "UTC"))

        run_once_url = "/".join(r.base_url.split("/")[:-1]) + "/runOnce"
        if command == "start":
            if run_once_url in api.get_job_urls():
                return Response(
                    ALREADY_STARTED_HTML,
                    status=200,
                    content_type="text/html",
                )
            api.register_dify_job(run_once_url, cron)
            return Response(
                START_HTML,
                status=200,
                content_type="text/html",
            )
        elif command == "stop":
            if run_once_url not in api.get_job_urls():
                return Response(
                    ALREADY_STOPPED_HTML,
                    status=200,
                    content_type="text/html",
                )
            api.delete_job_by_url(run_once_url)
            return Response(
                START_HTML,
                status=200,
                content_type="text/html",
            )
        elif command == "status":
            if run_once_url in api.get_job_urls():
                html = STATUS_ACTIVE_HTML
            else:
                html = STATUS_INACTIVE_HTML
            html = html.replace("{app-id}", app_id)
            html = html.replace("{now_utc}", datetime.datetime.now(tz=ZoneInfo("UTC")).strftime("%Y/%m/%d, %H:%M:%S"))
            html = html.replace(
                "{now}", datetime.datetime.now(tz=ZoneInfo(cron.timezone)).strftime("%Y/%m/%d, %H:%M:%S")
            )
            html = html.replace("{timezone}", cron.timezone)
            html = html.replace("{s}", str(cron.schedule["seconds"]))
            html = html.replace("{m}", str(cron.schedule["minutes"]))
            html = html.replace("{h}", str(cron.schedule["hours"]))
            html = html.replace("{d}", str(cron.schedule["mdays"]))
            html = html.replace("{months}", str(cron.schedule["months"]))
            html = html.replace("{w}", str(cron.schedule["wdays"]))
            return Response(
                html,
                status=200,
                content_type="text/html",
            )
        elif command == "runOnce":
            run_once(self.session, app_id)
            return Response(
                "OK",
                status=200,
                content_type="text/html",
            )
