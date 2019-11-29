import atexit
import logging
import time

import os
import threading

import click
from flask import Flask, render_template, request, url_for
from gevent.pywsgi import WSGIServer

from man.notebooker.serialization.serialization import get_serializer, serializer_kwargs_from_os_envs
from man.notebooker.constants import OUTPUT_BASE_DIR, \
    TEMPLATE_BASE_DIR, JobStatus, CANCEL_MESSAGE
from man.notebooker.utils.notebook_execution import mkdir_p, _cleanup_dirs
from man.notebooker.utils.templates import get_all_possible_templates
from man.notebooker.web.converters import DateConverter
from man.notebooker.web.report_hunter import _report_hunter
from man.notebooker.web.routes.core import core_bp
from man.notebooker.web.routes.prometheus import setup_metrics, prometheus_bp
from man.notebooker.web.routes.run_report import run_report_bp
from man.notebooker.web.routes.serve_results import serve_results_bp

flask_app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
all_report_refresher = None  # type: threading.Thread

flask_app.url_map.converters['date'] = DateConverter
flask_app.register_blueprint(core_bp)
flask_app.register_blueprint(prometheus_bp)
flask_app.register_blueprint(run_report_bp)
flask_app.register_blueprint(serve_results_bp)

# ----------------- Main page -------------------- #


@flask_app.route('/', methods=['GET'])
def index():
    username = request.headers.get("X-Auth-Username")
    all_reports = get_all_possible_templates()
    with flask_app.app_context():
        result = render_template('index.html',
                                 all_jobs_url=url_for("core_bp.all_available_results"),
                                 all_reports=all_reports,
                                 n_results_available=get_serializer().n_all_results(),
                                 donevalue=JobStatus.DONE,  # needed so we can check if a result is available
                                 username=username
                                 )
        return result


# ----------------- Flask admin ---------------- #

def _cancel_all_jobs():
    with flask_app.app_context():
        all_pending = get_serializer().get_all_results(
            mongo_filter={'status': {'$in': [JobStatus.SUBMITTED.value,JobStatus.PENDING.value]}})
        for result in all_pending:
            get_serializer().update_check_status(result.job_id, JobStatus.CANCELLED, error_info=CANCEL_MESSAGE)


@atexit.register
def _cleanup_on_exit():
    global all_report_refresher
    os.environ['NOTEBOOKER_APP_STOPPING'] = '1'
    _cancel_all_jobs()
    _cleanup_dirs()
    if all_report_refresher:
        # Wait until it terminates.
        logger.info('Stopping "report hunter" thread.')
        all_report_refresher.join()
    # Allow all clients looking for task results to get the bad news...
    time.sleep(2)


def start_app():
    global all_report_refresher
    if os.getenv('NOTEBOOKER_APP_STOPPING'):
        del os.environ['NOTEBOOKER_APP_STOPPING']
    logger.info('Creating %s', OUTPUT_BASE_DIR)
    mkdir_p(OUTPUT_BASE_DIR)
    logger.info('Creating %s', TEMPLATE_BASE_DIR)
    mkdir_p(TEMPLATE_BASE_DIR)

    setup_metrics(flask_app)
    all_report_refresher = threading.Thread(target=_report_hunter,
                                            kwargs=serializer_kwargs_from_os_envs())
    all_report_refresher.daemon = True
    all_report_refresher.start()


@click.command()
@click.option('--mongo-host', default='research')
@click.option('--database-name', default='mongoose_notebooker')
@click.option('--result-collection-name', default='NOTEBOOK_OUTPUT')
@click.option('--debug/--no-debug', default=False)
@click.option('--port', default=int(os.getenv('OCN_PORT', 11828)))
def main(mongo_host, database_name, result_collection_name, debug, port):
    logger.parent.setLevel(logging.DEBUG if debug else logging.INFO)

    # Used by Prometheus metrics
    env = {'mktdatad': 'dev', 'research': 'res', 'mktdatas': 'pre', 'mktdatap': 'prod'}.get(mongo_host, 'res')

    # Set out environment
    os.environ['NOTEBOOKER_ENVIRONMENT'] = os.getenv('NOTEBOOKER_ENVIRONMENT', env)
    os.environ['MONGO_HOST'] = os.getenv('MONGO_HOST', mongo_host)
    os.environ['DATABASE_NAME'] = os.getenv('DATABASE_NAME', database_name)
    os.environ['RESULT_COLLECTION_NAME'] = os.getenv('RESULT_COLLECTION_NAME', result_collection_name)

    logger.info('Running man.notebooker with params: '
                'mongo-host=%s, database-name=%s, '
                'result-collection-name=%s, debug=%s, '
                'port=%s', os.environ['MONGO_HOST'], os.environ['DATABASE_NAME'], os.environ['RESULT_COLLECTION_NAME'], debug, port)
    flask_app.config.update(
        TEMPLATES_AUTO_RELOAD=debug,
    )

    host = '0.0.0.0'
    start_app()
    http_server = WSGIServer((host, port), flask_app)
    http_server.serve_forever()


if __name__ == '__main__':
    main()
