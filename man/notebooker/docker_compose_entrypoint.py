import subprocess
import sys

from man.notebooker.execute_notebook import logger


def docker_compose_entrypoint():
    """
    Sadness. This is required because of https://github.com/jupyter/jupyter_client/issues/154
    Otherwise we will get "RuntimeError: Kernel died before replying to kernel_info"
    The suggested fix to use sh -c "command" does not work for our use-case, sadly.

    Examples
    --------
    man_execute_notebook --report-name watchdog_checks --mongo-host mktdatad
Recieved a request to run a report with the following parameters:
['/users/is/jbannister/pyenvs/notebooker/bin/python', '-m', 'man.notebooker.execute_notebook', '--report-name', 'watchdog_checks', '--mongo-host', 'mktdatad']
...

    man_execute_notebook
Recieved a request to run a report with the following parameters:
['/users/is/jbannister/pyenvs/notebooker/bin/python', '-m', 'man.notebooker.execute_notebook']
ValueError: Error! Please provide a --report-name.
    """
    args_to_execute = [sys.executable, '-m', __name__] + sys.argv[1:]
    logger.info('Recieved a request to run a report with the following parameters:')
    logger.info(args_to_execute)
    subprocess.Popen(args_to_execute).wait()
