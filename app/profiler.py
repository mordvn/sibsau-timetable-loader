import time
import asyncio
import atexit
from functools import wraps
from collections import defaultdict
from config import settings
import psutil
import os

from tabulate import tabulate

if settings.DEBUG:

    class Profiler:
        def __init__(self):
            self.function_stats = defaultdict(
                lambda: {
                    "total_time": 0,
                    "calls": 0,
                    "min_time": float("inf"),
                    "max_time": 0,
                }
            )
            self.process = psutil.Process(os.getpid())
            self.memory_samples = []
            self.cpu_samples = []
            self.sample_interval = 1.0  # секунды
            self.last_sample_time = time.time()

            atexit.register(self.print_stats)

        def add_execution_time(self, func_name, execution_time):
            stats = self.function_stats[func_name]
            stats["total_time"] += execution_time
            stats["calls"] += 1
            stats["min_time"] = min(stats["min_time"], execution_time)
            stats["max_time"] = max(stats["max_time"], execution_time)

            current_time = time.time()
            if current_time - self.last_sample_time >= self.sample_interval:
                self._sample_resources()
                self.last_sample_time = current_time

        def _sample_resources(self):
            # Получаем текущее использование памяти в МБ
            memory_mb = self.process.memory_info().rss / 1024 / 1024
            self.memory_samples.append(memory_mb)

            # Получаем текущее использование CPU в процентах
            try:
                cpu_percent = self.process.cpu_percent(interval=0.1)
                self.cpu_samples.append(cpu_percent)
            except:
                # Некоторые платформы могут иметь проблемы с cpu_percent
                pass

        def print_stats(self):
            if not self.function_stats:
                return

            # Берем финальный снимок использования ресурсов
            self._sample_resources()

            print("\n=== Function Execution Time Profile ===")

            sorted_stats = sorted(
                [(name, data) for name, data in self.function_stats.items()],
                key=lambda x: x[1]["total_time"],
                reverse=True,
            )

            table_data = []
            headers = [
                "Function",
                "Total Time (s)",
                "Calls",
                "Avg Time (s)",
                "Min Time (s)",
                "Max Time (s)",
            ]

            for func_name, stats in sorted_stats:
                avg_time = (
                    stats["total_time"] / stats["calls"] if stats["calls"] > 0 else 0
                )
                min_time = stats["min_time"] if stats["min_time"] != float("inf") else 0

                table_data.append(
                    [
                        func_name,
                        round(stats["total_time"], 4),
                        stats["calls"],
                        round(avg_time, 4),
                        round(min_time, 4),
                        round(stats["max_time"], 4),
                    ]
                )

            print(tabulate(table_data, headers=headers, tablefmt="grid"))

            print("\n=== System Resource Usage ===")

            resource_table = []
            resource_headers = ["Resource", "Average", "Peak", "Current"]

            if self.memory_samples:
                avg_memory = sum(self.memory_samples) / len(self.memory_samples)
                peak_memory = max(self.memory_samples)
                current_memory = self.memory_samples[-1]
                resource_table.append(
                    [
                        "Memory (MB)",
                        round(avg_memory, 2),
                        round(peak_memory, 2),
                        round(current_memory, 2),
                    ]
                )

            if self.cpu_samples:
                avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples)
                peak_cpu = max(self.cpu_samples)
                current_cpu = self.cpu_samples[-1]
                resource_table.append(
                    [
                        "CPU (%)",
                        round(avg_cpu, 2),
                        round(peak_cpu, 2),
                        round(current_cpu, 2),
                    ]
                )

            memory_info = psutil.virtual_memory()
            resource_table.append(
                ["System Memory (%)", "-", "-", round(memory_info.percent, 2)]
            )

            print(tabulate(resource_table, headers=resource_headers, tablefmt="grid"))

    profiler = Profiler()


def profile(func):
    if not settings.DEBUG:
        return func

    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = await func(*args, **kwargs)
            end_time = time.perf_counter()
            execution_time = end_time - start_time

            profiler.add_execution_time(func.__name__, execution_time)

            return result

        return async_wrapper
    else:

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            execution_time = end_time - start_time

            profiler.add_execution_time(func.__name__, execution_time)

            return result

        return sync_wrapper
