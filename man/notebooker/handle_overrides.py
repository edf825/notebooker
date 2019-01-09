import ast
import json
import os
import pickle
import subprocess
import sys
import tempfile
from typing import Tuple, Dict, Any, List, Union, AnyStr

import click
from ahl.logging import get_logger


logger = get_logger(__name__)


def _handle_overrides_safe(overrides, output_path):
    # type: (AnyStr, str) -> Dict[str, Union[Dict[str, Any], List[str]]]
    # This function executes the given python (in "overrides") and returns the
    # evaluated variables as a dictionary. Problems are returned as "issues" in a list.
    issues = []
    result = {'overrides': {}, 'issues': issues}
    try:
        raw_python = overrides.encode('utf-8')
    except UnicodeDecodeError:
        raw_python = str(overrides)
    logger.info('Parsing the following as raw python:\n{}'.format(raw_python))
    try:
        # Parse the python input as a Abstract Syntax Tree (this is what python itself does)
        parsed_module = ast.parse(raw_python)
        # Figure out what each node of the tree is doing (i.e. assigning, expression, etc)
        nodes = ast.iter_child_nodes(parsed_module)
        # Execute the code blindly. We trust the users (just about...) and are doing this in a safe-ish environment.
        exec(compile(parsed_module, filename='<ast>', mode='exec'))

        # Now, iterate through the nodes, figure out what was assigned, and add it to the 'overrides' dict.
        for node in nodes:
            if isinstance(node, (ast.Assign, ast.AugAssign)):
                targets = [_.id for _ in node.targets]
                logger.info('Found an assignment to: {}'.format(', '.join(targets)))
                for target in targets:
                    value = locals()[target]
                    result['overrides'][target] = value
                    try:
                        json.dumps(result['overrides'])  # Test that we can JSON serialise this - required by papermill
                    except TypeError as te:
                        # TODO: we may want to allow people to pass dataframes/timeseries as parameters. Can we handle?
                        issues.append('Could not JSON serialise a parameter ("{}") - this must be serialisable so that '
                                      'we can execute the notebook with it! (Error: {}, Value: {})'.format(
                            target, str(te), value))
            elif isinstance(node, (ast.Expr, ast.Expression)):
                issues.append('Found an expression that did nothing! It has a value of type: {}'.format(type(node.value)))
    except Exception as e:
        issues.append('An error was encountered: {}'.format(str(e)))

    if not issues:
        try:
            with open(output_path, 'w') as f:
                logger.info('Dumping to %s: %s', output_path, result)
                pickle.dump(result, f)
            return result
        except TypeError as e:
            issues.append('Could not pickle: {}. All input must be picklable (sorry!). '
                          'Error: {}'.format(str(result), str(e)))
    if issues:
        with open(output_path, 'w') as f:
            result = {'overrides': {}, 'issues': issues}
            logger.info('Dumping to %s: %s', output_path, result)
            pickle.dump(result, f)
    return result


def handle_overrides(overrides_string):
    # type: (str) -> Tuple[Dict[str, Any], List[str]]
    override_dict = {}
    issues = []
    if overrides_string.strip():
        tmp_file = tempfile.mktemp()
        try:
            subprocess.check_output([sys.executable, '-m',  __name__,
                                     '--overrides', overrides_string,
                                     '--output', tmp_file])
            with open(tmp_file, 'r') as f:
                output_dict = pickle.load(f)
            logger.info('Got %s from pickle', output_dict)
            override_dict, issues = output_dict['overrides'], output_dict['issues']
            os.remove(tmp_file)
        except subprocess.CalledProcessError as cpe:
            issues.append(str(cpe.output))
    return override_dict, issues


@click.command()
@click.option('--overrides')
@click.option('--output')
def main(overrides, output):
    return _handle_overrides_safe(overrides, output)


if __name__ == '__main__':
    main()
