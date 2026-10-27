import argparse
import logging

import uvicorn

from .api import create_app


def main():
    p = argparse.ArgumentParser(description="Async task console")
    p.add_argument("--db", default="tasks.db")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    uvicorn.run(create_app(args.db), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
