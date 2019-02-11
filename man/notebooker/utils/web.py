from __future__ import unicode_literals
from typing import AnyStr, List

from man.notebooker.constants import EMAIL_SPACE_ERR_MSG, FORBIDDEN_CHAR_ERR_MSG, FORBIDDEN_INPUT_CHARS


def _check_bad_chars(s, issues):
    # Checks from a set of forbidden characters
    if any(forbidden in s for forbidden in FORBIDDEN_INPUT_CHARS):
        issues.append(FORBIDDEN_CHAR_ERR_MSG.format(s, FORBIDDEN_INPUT_CHARS))


def validate_mailto(mailto, issues):
    # type: (AnyStr, List[AnyStr]) -> AnyStr
    if not mailto:
        return ''
    mailto = mailto.strip()
    if any(c.isspace() for c in mailto):
        issues.append(EMAIL_SPACE_ERR_MSG)
    _check_bad_chars(mailto, issues)
    return mailto


def validate_title(title, issues):
    # type: (AnyStr, List[AnyStr]) -> AnyStr
    out_s = title.strip()
    _check_bad_chars(out_s, issues)
    return out_s
