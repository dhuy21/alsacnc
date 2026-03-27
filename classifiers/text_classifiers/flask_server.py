import atexit
import logging
import os
import threading
from typing import Optional

import click
import service_streamer.service_streamer
from flask import Flask, Response, jsonify, request
from service_streamer import ThreadedStreamer

from classifiers.text_classifiers.model_wrappers import IECTModelWrapper
from shared_utils import load_yaml

logger = logging.getLogger("flask_server")

app = Flask(__name__)
streamer_ietc: Optional[ThreadedStreamer] = None


@app.route("/predict_ietc", methods=["POST"])
def stream_predict_ietc() -> Response:
    if streamer_ietc is None:
        return jsonify({"error": "Model not loaded yet"}), 503
    inputs = request.form.getlist("s")
    outputs = streamer_ietc.predict(inputs)
    return jsonify(outputs)


@app.route("/health", methods=["GET"])
def health() -> Response:
    return jsonify({"status": "ok", "worker": _worker_thread is not None and _worker_thread.is_alive()})


_worker_thread: Optional[threading.Thread] = None
_worker_shutdown = threading.Event()


def _stop_worker():
    """Signal the worker thread to stop gracefully."""
    _worker_shutdown.set()
    if _worker_thread and _worker_thread.is_alive():
        logger.info("Waiting for worker thread to stop...")
        _worker_thread.join(timeout=10)


def _start_worker_thread():
    """Start the classifiers worker as a background daemon thread."""
    global _worker_thread
    if os.environ.get("OPENWPM_STORAGE") != "postgres":
        return
    try:
        from classifiers.worker import run_worker

        _worker_thread = threading.Thread(
            target=run_worker,
            args=(_worker_shutdown,),
            daemon=True,
            name="classifiers-worker",
        )
        _worker_thread.start()
        atexit.register(_stop_worker)
    except Exception as e:
        logger.warning(f"Could not start worker thread: {e}")


@click.command()
@click.option("--config_path", type=str)
def main(config_path: str) -> None:
    config = load_yaml(config_path)
    model_ietc = IECTModelWrapper(
        two_step=config["two_step_model"],
        problem_type=config["problem_type"],
        model_type=config["model_type"],
        model_path=config["model_path"],
        model_path_2=config["model_path_2"],
        tokenizer_path=config["tokenizer_path"],
        tokenizer_args=config["tokenizer_args"],
        label2id=config["label2id"],
    )

    global streamer_ietc
    streamer_ietc = ThreadedStreamer(model_ietc.predict, batch_size=8, max_latency=0.1)

    _start_worker_thread()

    app.run(host="0.0.0.0", port=5001, debug=False)


if __name__ == "__main__":
    service_streamer.service_streamer.WORKER_TIMEOUT = 900
    main()
