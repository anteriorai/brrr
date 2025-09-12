import pytest


@pytest.fixture(params=("brrr-test", "'/:/\"~`\\", "🇰🇳"))
def topic(request):
    return request.param


@pytest.fixture(params=("task", "`'\"\\/~$!@:", "🏭"))
def task_name(request):
    return request.param
