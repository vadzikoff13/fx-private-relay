# Global fixture for Python tests
from typing import Generator
from unittest.mock import Mock, patch

import pytest
from twilio.rest import Client


@pytest.fixture(autouse=True)
def mock_twilio_client() -> Generator[Mock, None, None]:
    """
    Mock PhonesConfig with a mock twilio client
    """
    with patch(
        "phones.apps.PhonesConfig.twilio_client", spec_set=Client
    ) as mock_twilio_client:
        yield mock_twilio_client
