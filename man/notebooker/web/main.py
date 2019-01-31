import atexit
import logging
import time

import os
import shutil
import threading

import click
from ahl.logging import get_logger
from flask import Flask, render_template, request, g
from gevent.pywsgi import WSGIServer

from man.notebooker.utils.caching import get_cache, set_cache
from man.notebooker.constants import OUTPUT_BASE_DIR, \
    TEMPLATE_BASE_DIR, JobStatus, CANCEL_MESSAGE, CACHE_DIR
from man.notebooker.serialization.mongoose import NotebookResultSerializer
from man.notebooker.utils.results import all_available_results
from man.notebooker.utils.notebook_execution import mkdir_p
from man.notebooker.utils.templates import get_all_possible_checks
from man.notebooker.web.report_hunter import _report_hunter
from man.notebooker.web.routes.prometheus import setup_metrics, prometheus_bp
from man.notebooker.web.routes.run_report import run_report_bp
from man.notebooker.web.routes.serve_results import serve_results_bp

flask_app = Flask(__name__)
logger = get_logger(__name__)
all_report_refresher = None  # type: threading.Thread

flask_app.register_blueprint(prometheus_bp)
flask_app.register_blueprint(run_report_bp)
flask_app.register_blueprint(serve_results_bp)


def _result_serializer():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'result_serializer'):
        g.result_serializer = NotebookResultSerializer(
            mongo_host=get_cache('mongo_host'),
            database_name=get_cache('database_name'),
            result_collection_name=get_cache('result_collection_name'))
    return g.result_serializer

# ----------------- Main page -------------------- #


@flask_app.route('/', methods=['GET'])
def index():
    limit = int(request.args.get('limit', 50))
    cached = get_cache('index_limit_{}'.format(limit))
    if cached:
        return cached
    with flask_app.app_context():
        result =  render_template('index.html',
                                  all_jobs=all_available_results(_result_serializer(), limit),
                                  all_reports=get_all_possible_checks(),
                                  n_results_available=_result_serializer().n_all_results(),
                                  donevalue=JobStatus.DONE,  # needed so we can check if a result is available
                                  )
        set_cache('index_limit_{}'.format(limit), result, timeout=10)
        return result


# ----------------- Flask admin ---------------- #

def _cancel_all_jobs():
    with flask_app.app_context():
        all_pending = _result_serializer().get_all_results(
            mongo_filter={'status': {'$in': [JobStatus.SUBMITTED.value,JobStatus.PENDING.value]}})
        for result in all_pending:
            _result_serializer().update_check_status(result.job_id, JobStatus.CANCELLED, error_info=CANCEL_MESSAGE)


@atexit.register
def _cleanup_on_exit():
    global all_report_refresher
    set_cache('_STILL_ALIVE', False)
    _cancel_all_jobs()
    shutil.rmtree(OUTPUT_BASE_DIR)
    shutil.rmtree(TEMPLATE_BASE_DIR)
    shutil.rmtree(CACHE_DIR)
    if all_report_refresher:
        # Wait until it terminates.
        logger.info('Stopping "report hunter" thread.')
        all_report_refresher.join()
    # Allow all clients looking for task results to get the bad news...
    time.sleep(2)


def start_app():
    set_cache('_STILL_ALIVE', True)
    global all_report_refresher
    logger.info('Creating %s', OUTPUT_BASE_DIR)
    mkdir_p(OUTPUT_BASE_DIR)
    logger.info('Creating %s', TEMPLATE_BASE_DIR)
    mkdir_p(TEMPLATE_BASE_DIR)

    setup_metrics(flask_app)
    all_report_refresher = threading.Thread(target=_report_hunter,
                                            args=(get_cache('mongo_host'),
                                                  get_cache('database_name'),
                                                  get_cache('result_collection_name')))
    all_report_refresher.daemon = True
    all_report_refresher.start()


@click.command()
@click.option('--mongo-host', default='research')
@click.option('--database-name', default='mongoose_restech')
@click.option('--result-collection-name', default='NOTEBOOK_OUTPUT')
@click.option('--debug/--no-debug', default=False)
@click.option('--port', default=int(os.getenv('OCN_PORT', 11828)))
def main(mongo_host, database_name, result_collection_name, debug, port):
    logger.info('Running man.notebooker with params: '
                'mongo-host=%s, database-name=%s, '
                'result-collection-name=%s, debug=%s, '
                'port=%s', mongo_host, database_name, result_collection_name, debug, port)
    logger.parent.setLevel(logging.DEBUG if debug else logging.INFO)

    # Used by Prometheus metrics
    env = {'mktdatad': 'dev', 'research': 'res', 'mktdatas': 'pre', 'mktdatap': 'prod'}.get(mongo_host, 'research')
    set_cache('env', env)
    set_cache('mongo_host', mongo_host)
    set_cache('database_name', database_name)
    set_cache('result_collection_name', result_collection_name)

    host = '0.0.0.0'
    start_app()
    http_server = WSGIServer((host, port), flask_app)
    http_server.serve_forever()


if __name__ == '__main__':
    main()
