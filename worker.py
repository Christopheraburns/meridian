"""Launch as a separate Docker service or an approved Workbench Job."""
import argparse
from pathlib import Path
import signal
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent/'src'))
from meridian.jobs import Settings, Worker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--once',action='store_true',help='Recover interrupted jobs, then process at most one queued task')
    args = parser.parse_args()
    worker = Worker(Settings.environment())
    def stop(signum, frame): worker.stopping = True
    signal.signal(signal.SIGTERM,stop)
    signal.signal(signal.SIGINT,stop)
    worker.run(once=args.once)


if __name__=='__main__': main()
