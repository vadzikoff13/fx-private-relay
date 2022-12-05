"""Tests for phones/cleaners.py"""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import Mock

from django.utils import timezone

from twilio.rest import Client

from model_bakery import baker
from typing import Any
import pytest

from phones.cleaners import RelayNumberSyncChecker
from phones.models import RealPhone, RelayNumber

from .models_tests import make_phone_test_user


TEST_MAIN_NUMBER = "+12005550000"


@pytest.fixture(autouse=True)
def main_number(settings) -> str:
    settings.TWILIO_MAIN_NUMBER = TEST_MAIN_NUMBER
    return TEST_MAIN_NUMBER


def setup_relay_number_test_data(
    mock_twilio_client: Client,
    main_number_in_twilio: bool = True,
    remove_number_from_relay: bool = False,
    remove_number_from_twilio: bool = False,
) -> None:
    """Setup Relay Numbers and Twilio Response for testing."""

    verification_base_date = timezone.now() - timedelta(days=60)
    twilio_objects: list[Mock] = []
    if main_number_in_twilio:
        twilio_objects.append(Mock(phone_number=TEST_MAIN_NUMBER))

    test_data: dict[str, dict[str, Any]] = {
        "+13015550001": {},
        "+13015550002": {"texts_forwarded": 1},
        "+13015550003": {"texts_blocked": 1},
        "+13015550004": {"calls_forwarded": 1},
        "+13015550005": {"calls_blocked": 1},
        "+13015550006": {"texts_forwarded": 1, "calls_forwarded": 1},
        "+13015550007": {"enabled": False},
    }

    if remove_number_from_relay:
        test_data["+13015550002"]["in_relay"] = False
    if remove_number_from_twilio:
        test_data["+13015550003"]["in_twilio"] = False

    for num, (relay_number, data) in enumerate(test_data.items()):
        user = make_phone_test_user()
        real_number = relay_number.replace("+1301", "+1201")
        verification_date = verification_base_date + timedelta(seconds=60 * num)
        baker.make(
            RealPhone,
            user=user,
            number=real_number,
            verification_sent_date=verification_date,
            verified=True,
        )
        in_relay = test_data.pop("in_relay", True)
        in_twilio = test_data.pop("in_twilio", True)
        if in_relay:
            baker.make(RelayNumber, user=user, number=relay_number, **data)
        if in_twilio:
            twilio_objects.append(Mock(phone_number=relay_number))
    mock_twilio_client.incoming_phone_numbers.list.return_value = twilio_objects


@pytest.mark.django_db
def test_relay_number_sync_checker_no_data() -> None:
    """RelayNumberSyncChecker works on an empty database."""
    checker = RelayNumberSyncChecker()
    assert checker.issues() == 1
    assert checker.counts == {
        "summary": {"ok": 0, "needs_cleaning": 1},
        "relay_numbers": {
            "all": 0,
            "disabled": 0,
            "enabled": 0,
            "used": 0,
            "used_both": 0,
            "used_texts": 0,
            "used_calls": 0,
        },
        "twilio_numbers": {"all": 0, "main_number": 0},
        "sync_check": {
            "all": 0,
            "in_both_db": 0,
            "main_number": 0,
            "only_relay_db": 0,
            "only_twilio_db": 0,
        },
    }
    assert checker.clean() == 0
    report = checker.markdown_report()
    expected = """\
Relay Numbers:
  All: 0
Twilio Numbers:
  All: 0
Sync Check:
  All: 0"""
    assert report == expected


@pytest.mark.django_db
def test_relay_number_sync_checker_just_main(mock_twilio_client) -> None:
    """RelayNumberSyncChecker works on an empty database."""
    mock_twilio_client.incoming_phone_numbers.list.return_value = [
        Mock(phone_number=TEST_MAIN_NUMBER)
    ]
    checker = RelayNumberSyncChecker()
    assert checker.issues() == 0
    assert checker.counts == {
        "summary": {"ok": 0, "needs_cleaning": 0},
        "relay_numbers": {
            "all": 0,
            "disabled": 0,
            "enabled": 0,
            "used": 0,
            "used_both": 0,
            "used_texts": 0,
            "used_calls": 0,
        },
        "twilio_numbers": {"all": 1, "main_number": 1},
        "sync_check": {
            "all": 1,
            "in_both_db": 0,
            "main_number": 1,
            "only_relay_db": 0,
            "only_twilio_db": 0,
        },
    }
    assert checker.clean() == 0
    report = checker.markdown_report()
    expected = """\
Relay Numbers:
  All: 0
Twilio Numbers:
  All: 1
    Main Number: 1 (100.0%)
Sync Check:
  All: 1
    In Both Databases      : 0 (  0.0%)
    Main Number in Twilio  : 1 (100.0%)
    Only in Relay Database : 0 (  0.0%)
    Only in Twilio Database: 0 (  0.0%)"""
    assert report == expected


@pytest.mark.django_db
def test_relay_number_sync_checker_synced_with_twilio(mock_twilio_client) -> None:
    """RelayNumberSyncChecker detects that all phone numbers are synced."""
    setup_relay_number_test_data(mock_twilio_client)
    checker = RelayNumberSyncChecker()
    assert checker.issues() == 0
    assert checker.counts == {
        "summary": {"ok": 7, "needs_cleaning": 0},
        "relay_numbers": {
            "all": 7,
            "disabled": 1,
            "enabled": 6,
            "used": 5,
            "used_both": 1,
            "used_texts": 2,
            "used_calls": 2,
        },
        "twilio_numbers": {"all": 8, "main_number": 1},
        "sync_check": {
            "all": 8,
            "in_both_db": 7,
            "main_number": 1,
            "only_relay_db": 0,
            "only_twilio_db": 0,
        },
    }
    assert checker.clean() == 0
    report = checker.markdown_report()
    expected = """\
Relay Numbers:
  All: 7
    Enabled: 6 (85.7%)
      Used: 5 (83.3%)
        Used for Texts Only: 2 (40.0%)
        Used for Calls Only: 2 (40.0%)
        Used for Both      : 1 (20.0%)
Twilio Numbers:
  All: 8
    Main Number: 1 (12.5%)
Sync Check:
  All: 8
    In Both Databases      : 7 (87.5%)
    Main Number in Twilio  : 1 (12.5%)
    Only in Relay Database : 0 ( 0.0%)
    Only in Twilio Database: 0 ( 0.0%)"""
    assert report == expected


@pytest.mark.django_db
def test_relay_number_sync_checker_main_not_in_twilio(mock_twilio_client) -> None:
    """RelayNumberSyncChecker detects that all phone numbers are synced."""
    setup_relay_number_test_data(mock_twilio_client, main_number_in_twilio=False)
    checker = RelayNumberSyncChecker()
    assert checker.issues() == 1
    assert checker.counts == {
        "summary": {"ok": 7, "needs_cleaning": 1},
        "relay_numbers": {
            "all": 7,
            "disabled": 1,
            "enabled": 6,
            "used": 5,
            "used_both": 1,
            "used_texts": 2,
            "used_calls": 2,
        },
        "twilio_numbers": {"all": 7, "main_number": 0},
        "sync_check": {
            "all": 7,
            "in_both_db": 7,
            "main_number": 0,
            "only_relay_db": 0,
            "only_twilio_db": 0,
        },
    }
    assert checker.clean() == 0
    report = checker.markdown_report()
    expected = """\
Relay Numbers:
  All: 7
    Enabled: 6 (85.7%)
      Used: 5 (83.3%)
        Used for Texts Only: 2 (40.0%)
        Used for Calls Only: 2 (40.0%)
        Used for Both      : 1 (20.0%)
Twilio Numbers:
  All: 7
    Main Number: 0 (0.0%)
Sync Check:
  All: 7
    In Both Databases      : 7 (100.0%)
    Main Number in Twilio  : 0 (  0.0%)
    Only in Relay Database : 0 (  0.0%)
    Only in Twilio Database: 0 (  0.0%)"""
    assert report == expected
