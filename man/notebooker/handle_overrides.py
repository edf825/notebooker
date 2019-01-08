import ast
import cPickle
import importlib
import json
import os
import subprocess
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
    logger.info('Parsing the following as python: {}'.format(raw_python))
    try:
        parsed_module = ast.parse(raw_python)
        nodes = ast.iter_child_nodes(parsed_module)
        exec(compile(parsed_module, filename='<ast>', mode='exec'))
        for node in nodes:
            if isinstance(node, (ast.Assign, ast.AugAssign)):
                targets = [_.id for _ in node.targets]
                logger.info('Found an assignment to: {}'.format(', '.join(targets)))
                for target in targets:
                    value = locals()[target]
                    try:
                        json.dumps(value)  # Test that we can JSON serialise this - required by papermill
                    except TypeError as te:
                        issues.append('Could not JSON serialise a parameter ("{}") - this must be serialisable so that '
                                      'we can execute the notebook with it! (Error: {}, Value: {})'.format(
                            target, str(te), value))
                        continue
                    result['overrides'][target] = value
            elif isinstance(node, (ast.Expr, ast.Expression)):
                issues.append('Found an expression that did nothing! It has a value of type: {}'.format(type(node.value)))
    except Exception as e:
        issues.append('An error was encountered: {}'.format(str(e)))

    if not issues:
        try:
            with open(output_path, 'w') as f:
                cPickle.dump(result, f)
            return result
        except TypeError as e:
            issues.append('Could not pickle: {}. All input must be picklable (sorry!). '
                          'Error: {}'.format(str(result), str(e)))

    with open(output_path, 'w') as f:
        cPickle.dump({'overrides': {},
                      'issues': issues},
                     f)
    return result


def _handle_overrides(overrides_string):
    # type: (str) -> Tuple[Dict[str, Any], List[str]]
    override_dict = {}
    issues = []
    tmp_file = tempfile.mktemp()
    try:
        process = subprocess.Popen(['python', '-m',  __name__,
                                    '--overrides', overrides_string,
                                    '--output', tmp_file])
        process.wait()
        with open(tmp_file, 'r') as f:
            output_dict = cPickle.load(f)
        override_dict, issues = output_dict['overrides'], output_dict['issues']
        os.remove(tmp_file)
    except subprocess.CalledProcessError as cpe:
        issues.append(str(cpe))
    return override_dict, issues


@click.command()
@click.option('--overrides')
@click.option('--output')
def main(overrides, output):
    return _handle_overrides_safe(overrides, output)


if __name__ == '__main__':
    main()
