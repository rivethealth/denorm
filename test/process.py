import subprocess
import typing


def run_process(args: typing.List[str], env=None) -> bytes:
    run = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        env=env,
    )
    if run.returncode:
        raise Exception(f"Exited with code {run.returncode}")
    return run.stdout
