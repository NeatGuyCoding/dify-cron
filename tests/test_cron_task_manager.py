import pathlib
import sys
import threading
import time
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from endpoints.cron import CronTaskManager, Cron


class TestCronTaskManager:
    """Test CronTaskManager class functionality"""
    
    def setup_method(self):
        """Setup before each test method"""
        self.task_manager = CronTaskManager()
        self.mock_session = Mock()
        self.mock_cron = Mock()
        self.mock_cron.cron_str = "0 0 * * * *"
    
    def teardown_method(self):
        """Cleanup after each test method"""
        # Clean up all tasks
        running_tasks = self.task_manager.get_running_tasks()
        for app_id in list(running_tasks.keys()):
            self.task_manager.stop_cron_task(app_id)
    
    def test_start_cron_task_success(self):
        """Test successful cron task start"""
        app_id = "test_app_1"
        
        success = self.task_manager.start_cron_task(app_id, self.mock_session, self.mock_cron)
        
        assert success is True
        assert self.task_manager.is_task_running(app_id)
        assert app_id in self.task_manager.get_running_tasks()
    
    def test_start_cron_task_duplicate(self):
        """Test duplicate start of same app task"""
        app_id = "test_app_1"
        
        # First start
        success1 = self.task_manager.start_cron_task(app_id, self.mock_session, self.mock_cron)
        assert success1 is True
        
        # Second start should fail
        success2 = self.task_manager.start_cron_task(app_id, self.mock_session, self.mock_cron)
        assert success2 is False
        
        # Ensure only one task is running
        running_tasks = self.task_manager.get_running_tasks()
        assert len(running_tasks) == 1
        assert app_id in running_tasks
    
    def test_stop_cron_task_success(self):
        """Test successful cron task stop"""
        app_id = "test_app_1"
        
        # Start task
        self.task_manager.start_cron_task(app_id, self.mock_session, self.mock_cron)
        assert self.task_manager.is_task_running(app_id)
        
        # Stop task
        success = self.task_manager.stop_cron_task(app_id)
        assert success is True
        assert not self.task_manager.is_task_running(app_id)
    
    def test_stop_cron_task_not_running(self):
        """Test stopping non-existent task"""
        app_id = "non_existent_app"
        
        success = self.task_manager.stop_cron_task(app_id)
        assert success is False
    
    def test_multiple_apps_concurrent(self):
        """Test concurrent management of multiple apps"""
        app_ids = [f"app_{i}" for i in range(5)]
        
        # Concurrently start multiple tasks
        threads = []
        results = []
        
        def start_task(app_id):
            result = self.task_manager.start_cron_task(app_id, self.mock_session, self.mock_cron)
            results.append((app_id, result))
        
        for app_id in app_ids:
            thread = threading.Thread(target=start_task, args=(app_id,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Verify all tasks started successfully
        assert len(results) == 5
        assert all(result for _, result in results)
        
        running_tasks = self.task_manager.get_running_tasks()
        assert len(running_tasks) == 5
        assert all(app_id in running_tasks for app_id in app_ids)
        
        # Test concurrent stopping
        stop_threads = []
        stop_results = []
        
        def stop_task(app_id):
            result = self.task_manager.stop_cron_task(app_id)
            stop_results.append((app_id, result))
        
        for app_id in app_ids:
            thread = threading.Thread(target=stop_task, args=(app_id,))
            stop_threads.append(thread)
            thread.start()
        
        for thread in stop_threads:
            thread.join()
        
        # Verify all tasks stopped successfully
        assert len(stop_results) == 5
        assert all(result for _, result in stop_results)
        
        running_tasks = self.task_manager.get_running_tasks()
        assert len(running_tasks) == 0
    
    def test_task_isolation(self):
        """Test isolation between tasks"""
        app_id_1 = "app_1"
        app_id_2 = "app_2"
        
        # Start two tasks
        self.task_manager.start_cron_task(app_id_1, self.mock_session, self.mock_cron)
        self.task_manager.start_cron_task(app_id_2, self.mock_session, self.mock_cron)
        
        # Verify both tasks are running
        assert self.task_manager.is_task_running(app_id_1)
        assert self.task_manager.is_task_running(app_id_2)
        
        # Stop one task
        self.task_manager.stop_cron_task(app_id_1)
        
        # Verify the other task is unaffected
        assert not self.task_manager.is_task_running(app_id_1)
        assert self.task_manager.is_task_running(app_id_2)
    
    def test_get_running_tasks(self):
        """Test getting running tasks list"""
        # Initial state should be empty
        assert len(self.task_manager.get_running_tasks()) == 0
        
        # Add tasks
        app_ids = ["app_1", "app_2", "app_3"]
        for app_id in app_ids:
            self.task_manager.start_cron_task(app_id, self.mock_session, self.mock_cron)
        
        # Verify task list
        running_tasks = self.task_manager.get_running_tasks()
        assert len(running_tasks) == 3
        assert all(app_id in running_tasks for app_id in app_ids)
        
        # Stop one task
        self.task_manager.stop_cron_task("app_1")
        
        # Verify task list updated
        running_tasks = self.task_manager.get_running_tasks()
        assert len(running_tasks) == 2
        assert "app_1" not in running_tasks
        assert "app_2" in running_tasks
        assert "app_3" in running_tasks


class TestCronIntegration:
    """Test Cron class integration with task manager"""
    
    def test_cron_validation(self):
        """Test cron expression validation"""
        # Valid cron expressions
        valid_crons = [
            "0 0 * * * *",  # Every hour
            "*/5 * * * * *",  # Every 5 seconds
            "0 */10 * * * *",  # Every 10 minutes
            "0 0 0 * * *",  # Daily at midnight
        ]
        
        for cron_str in valid_crons:
            cron = Cron(cron_str)
            assert cron.cron_str == cron_str
            # Verify is_now_to_call can be called without exception
            try:
                cron.is_now_to_call()
            except Exception as e:
                pytest.fail(f"Valid cron '{cron_str}' should not raise exception: {e}")
    
    def test_cron_invalid_expressions(self):
        """Test invalid cron expressions"""
        invalid_crons = [
            "*/0 * * * * *",  # Step size 0
            "* * * */0 * *",  # Day step size 0
            "invalid cron",  # Completely invalid
            "* * * * *",  # Missing seconds field
            "* * * * * * *",  # Too many fields
        ]
        
        for cron_str in invalid_crons:
            with pytest.raises(Exception):
                Cron(cron_str).is_now_to_call()
    
    def test_cron_schedule_calculation(self):
        """Test cron schedule calculation"""
        cron = Cron("*/5 */10 */2 * * *")
        schedule = cron.calc_schedule()
        
        # Verify schedule structure
        assert "timezone" in schedule
        assert "seconds" in schedule
        assert "minutes" in schedule
        assert "hours" in schedule
        assert "mdays" in schedule
        assert "months" in schedule
        assert "wdays" in schedule
        
        # Verify step calculation
        assert schedule["seconds"] == [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        assert schedule["minutes"] == [0, 10, 20, 30, 40, 50]
        assert schedule["hours"] == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]


class TestIndependentThreadBehavior:
    """Test independent thread behavior"""
    
    def test_unlimited_thread_creation(self):
        """Test that we can create unlimited independent threads"""
        task_manager = CronTaskManager()
        
        # Create many tasks (more than typical thread pool limits)
        num_tasks = 50
        app_ids = [f"app_{i}" for i in range(num_tasks)]
        
        # Start all tasks
        for app_id in app_ids:
            success = task_manager.start_cron_task(app_id, Mock(), Mock())
            assert success is True
        
        # Verify all tasks can start (no thread pool limitations)
        running_tasks = task_manager.get_running_tasks()
        assert len(running_tasks) == num_tasks
        assert task_manager.get_task_count() == num_tasks
        
        # Verify all threads are independent
        for app_id in app_ids:
            assert task_manager.is_task_running(app_id)
        
        # Cleanup
        for app_id in app_ids:
            task_manager.stop_cron_task(app_id)
    
    def test_daemon_thread_behavior(self):
        """Test daemon thread behavior"""
        task_manager = CronTaskManager()
        
        # Start a task
        app_id = "test_daemon_app"
        success = task_manager.start_cron_task(app_id, Mock(), Mock())
        assert success is True
        
        # Get the thread
        running_tasks = task_manager.get_running_tasks()
        thread = running_tasks[app_id]
        
        # Verify it's a daemon thread
        assert thread.daemon is True
        assert thread.is_alive() is True
        assert thread.name == f"cron-{app_id}"
        
        # Stop the task
        task_manager.stop_cron_task(app_id)
        assert not task_manager.is_task_running(app_id)
    
    def test_thread_isolation(self):
        """Test that each cron task runs in its own thread"""
        task_manager = CronTaskManager()
        
        # Start multiple tasks
        app_ids = ["app_1", "app_2", "app_3"]
        for app_id in app_ids:
            task_manager.start_cron_task(app_id, Mock(), Mock())
        
        # Verify each task has its own thread
        running_tasks = task_manager.get_running_tasks()
        thread_ids = set()
        
        for app_id, thread in running_tasks.items():
            assert thread.is_alive()
            assert thread.name == f"cron-{app_id}"
            thread_ids.add(thread.ident)
        
        # Verify all threads are different
        assert len(thread_ids) == len(app_ids)
        
        # Cleanup
        for app_id in app_ids:
            task_manager.stop_cron_task(app_id)
