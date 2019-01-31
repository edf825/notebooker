# -*- coding: utf-8 -*-
import pytest

from man.notebooker import constants
from man.notebooker.utils import web


@pytest.mark.parametrize('test_name, mailto, expected_issues, expected_mailto', [
    ('simple email',
     'hello@ahl.com', [], 'hello@ahl.com'),
    ('simple email, trailing spaces',
     'hello@ahl.com   \n', [], 'hello@ahl.com'),
    ('space in email',
     'hello @ahl.com', [constants.EMAIL_SPACE_ERR_MSG], 'hello @ahl.com'),
    ('apostrophe',
     "o'neill@ahl.com", [], 'o\'neill@ahl.com'),
    ('weird email address',
     u'å∂ßåß∂@ahl.com', [], u'å∂ßåß∂@ahl.com'),
    ('forbidden char',
     'o"neill@ahl.com', ['This report has an invalid input (o"neill@ahl.com) - it must not contain any of [\'"\'].'], 'o\"neill@ahl.com'),
])
def test_validate_mailto(test_name, mailto, expected_issues, expected_mailto):
    issues = []
    actual_mailto = web.validate_mailto(mailto, issues)
    assert issues == expected_issues
    assert actual_mailto == expected_mailto.encode('utf-8')


@pytest.mark.parametrize('test_name, title, expected_issues, expected_mailto', [
    ('simple title',
     'adasdasda', [], 'adasdasda'),
    ('title with emojis',
     u'😀 😁 😂', [], u'😀 😁 😂'),
    ('apostrophe',
     "''''''''''''''", [], "''''''''''''''"),
    ('forbidden char',
     'this is "great"', ['This report has an invalid input (this is "great") - it must not contain any of [\'"\'].'], 'this is "great"'),
])
def test_validate_title(test_name, title, expected_issues, expected_mailto):
    issues = []
    actual_title = web.validate_title(title, issues)
    assert issues == expected_issues
    assert actual_title == expected_mailto.encode('utf-8')
