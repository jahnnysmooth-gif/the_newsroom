import asyncio
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
NEWS_BOT_SCRIPT = ROOT / "news_bot" / "news_bot.py"
PERFORMANCE_BOT_SCRIPT = ROOT / "performance_bot" / "performance_bot.py"


async def stream_output(prefix: str, stream: asyncio.StreamReader) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        print(f"[{prefix}] {line.decode(errors='replace').rstrip()}", flush=True)


async def run_script_forever(name: str, script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"{name} script not found: {script_path}")

    restart_delay = 30
    max_restart_delay = 300

    while True:
        print(f"[MAIN] Starting {name}: {script_path}", flush=True)

        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script_path),
            cwd=str(script_path.parent),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=os.environ.copy(),
        )

        started_at = asyncio.get_running_loop().time()

        assert proc.stdout is not None
        output_task = asyncio.create_task(stream_output(name, proc.stdout))

        return_code = await proc.wait()
        await output_task

        runtime_seconds = int(asyncio.get_running_loop().time() - started_at)
        if runtime_seconds >= 300:
            restart_delay = 30
        else:
            restart_delay = min(restart_delay * 2, max_restart_delay)

        print(
            f"[MAIN] {name} exited with code {return_code} after {runtime_seconds}s. "
            f"Restarting in {restart_delay} seconds...",
            flush=True,
        )
        await asyncio.sleep(restart_delay)


async def main() -> None:
    print("=== THE_NEWSROOM MAIN.PY STARTING ===", flush=True)
    print(f"[MAIN] Python version: {sys.version}", flush=True)
    print(f"[MAIN] Working directory: {ROOT}", flush=True)

    tasks = [
        asyncio.create_task(run_script_forever("news_bot", NEWS_BOT_SCRIPT)),
        asyncio.create_task(run_script_forever("performance_bot", PERFORMANCE_BOT_SCRIPT)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[MAIN] Shutdown requested", flush=True)
