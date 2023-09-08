import pytest, json, sys
from celery.result import AsyncResult
sys.path.append('app')
import notifier.notifier as notifier 

def test_notifier():
    '''Formats the directory and verifies that all files ended up where they should be'''
    result = notifier.send_error_notification("Test Subject from Transfer Service", "Test Body from Transfer Service", "dts@hu.onmicrosoft.com")
    assert isinstance(result, AsyncResult)
