import asyncio
import json
import random
import time
import uuid
from collections import deque
from typing import Optional

import httpx

from config import APP_BASE_URL, BUCKET_WINDOW_SECONDS, SIMULATION_BUS_COUNT, SIMULATION_MAX_CONCURRENCY, SIMULATION_MIN_INTERVAL_S, SIMULATION_MAX_INTERVAL_S

simulation_jobs: dict[str, dict] = {}
active_job_id: Optional[str] = None


def random_scan_payload(index: int) -> dict:
    bus_id = f"ONIBUS-{random.randint(1, SIMULATION_BUS_COUNT):03d}"
    return {
        "student_id": f"SIM-ALUNO-{index:06d}",
        "bus_id": bus_id,
        "monitor_id": "SIM-MONITOR",
    }


async def run_simulation_loop(job_id: str, min_count: int, max_count: int) -> None:
    job = simulation_jobs[job_id]
    queue: asyncio.Queue = job["queue"]
    stop_event: asyncio.Event = job["stop_event"]

    semaphore = asyncio.Semaphore(SIMULATION_MAX_CONCURRENCY)
    timestamps: deque = deque()
    counters = {"sent": 0, "success": 0, "fail": 0}
    started_at = time.time()
    phase_info = {"phase": "waiting", "burst_size": 0, "next_at": started_at}

    def build_buckets(now: float) -> list:
        buckets = [0] * BUCKET_WINDOW_SECONDS
        recent_timestamps = [
            ts for ts in timestamps if 0 <= now - ts < BUCKET_WINDOW_SECONDS
        ]
        timestamps.clear()
        timestamps.extend(recent_timestamps)

        for ts in recent_timestamps:
            age = max(0.0, now - ts)
            age_bucket = min(BUCKET_WINDOW_SECONDS - 1, int(age))
            idx = BUCKET_WINDOW_SECONDS - 1 - age_bucket
            buckets[idx] += 1
        return buckets

    async def emit():
        now = time.time()
        next_in = round(max(0.0, phase_info["next_at"] - now), 1) if phase_info["phase"] == "waiting" else 0
        await queue.put({
            "done": False,
            "phase": phase_info["phase"],
            "burst_size": phase_info["burst_size"],
            "next_in": next_in,
            "sent": counters["sent"],
            "success": counters["success"],
            "fail": counters["fail"],
            "buckets": build_buckets(now),
            "elapsed": round(now - started_at, 1),
        })

    async def fire_one(client: httpx.AsyncClient, index: int):
        if stop_event.is_set():
            return
        async with semaphore:
            if stop_event.is_set():
                return
            payload = random_scan_payload(index)
            try:
                response = await client.post("/scan", json=payload, timeout=10.0)
                ok = response.status_code == 200
            except Exception:
                ok = False
            counters["sent"] += 1
            counters["success" if ok else "fail"] += 1
            timestamps.append(time.time())

    async def emitter_loop():
        while True:
            await emit()
            await asyncio.sleep(0.3)

    emitter_task = asyncio.create_task(emitter_loop())

    try:
        async with httpx.AsyncClient(base_url=APP_BASE_URL) as client:
            burst_size = random.randint(min_count, max_count)
            phase_info["phase"] = "bursting"
            phase_info["burst_size"] = burst_size

            base_index = counters["sent"]
            tasks = [fire_one(client, base_index + i) for i in range(burst_size)]
            await asyncio.gather(*tasks)
    finally:
        emitter_task.cancel()
        try:
            await emitter_task
        except asyncio.CancelledError:
            pass
        now = time.time()
        await queue.put({
            "done": True,
            "phase": "stopped",
            "burst_size": 0,
            "next_in": 0,
            "sent": counters["sent"],
            "success": counters["success"],
            "fail": counters["fail"],
            "buckets": build_buckets(now),
            "elapsed": round(now - started_at, 1),
        })


async def start_simulation(min_count: int, max_count: int) -> str:
    global active_job_id
    job_id = uuid.uuid4().hex
    simulation_jobs[job_id] = {"queue": asyncio.Queue(), "stop_event": asyncio.Event()}
    active_job_id = job_id
    simulation_jobs[job_id]["task"] = asyncio.create_task(
        run_simulation_loop(job_id, min_count, max_count)
    )
    return job_id


def stop_simulation(job_id: str) -> None:
    job = simulation_jobs.get(job_id)
    if job is not None:
        job["stop_event"].set()
        task = job.get("task")
        if task is not None and not task.done():
            task.cancel()


async def event_stream(job_id: str):
    global active_job_id
    queue: asyncio.Queue = simulation_jobs[job_id]["queue"]
    try:
        while True:
            message = await queue.get()
            yield f"data: {json.dumps(message)}\n\n"
            if message.get("done"):
                break
    finally:
        simulation_jobs.pop(job_id, None)
        if active_job_id == job_id:
            active_job_id = None
