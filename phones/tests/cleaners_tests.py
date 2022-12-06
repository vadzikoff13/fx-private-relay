"""Tests for phones/cleaners.py"""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import Mock
from uuid import uuid4

from django.utils import timezone

from twilio.rest import Client

from model_bakery import baker
from typing import Any
import pytest

from phones.cleaners import RelayNumberSyncChecker
from phones.models import RealPhone, RelayNumber

from .models_tests import make_phone_test_user


@pytest.fixture(autouse=True)
def mock_twilio_settings(settings) -> None:
    settings.TWILIO_ACCOUNT_SID = f"AC{uuid4().hex}"
    settings.TWILIO_SMS_APPLICATION_SID = f"AP{uuid4().hex}"
    settings.TWILIO_MESSAGING_SERVICE_SID = f"MG{uuid4().hex}"
    settings.TWILIO_MAIN_NUMBER = "+12005550000"


@pytest.fixture
def setup_relay_number_test_data(
    mock_twilio_client: Client,
    db,
    request,
    settings,
) -> None:
    """Setup Relay Numbers and Twilio Response for testing."""
    test_mark = request.node.get_closest_marker("relay_test_data")
    config = test_mark.kwargs if test_mark else {}
    main_number_in_twilio = config.get("main_number_in_twilio", True)
    remove_number_from_twilio = config.get("remove_number_from_twilio", False)
    remove_number_from_relay = config.get("remove_number_from_relay", False)

    verification_base_date = timezone.now() - timedelta(days=60)
    twilio_objects: list[Mock] = []
    if main_number_in_twilio:
        twilio_objects.append(
            create_mock_number_instance(settings.TWILIO_MAIN_NUMBER, settings)
        )

    test_data: dict[str, dict[str, Any]] = {
        "+13015550001": {},
        "+13015550002": {"texts_forwarded": 1},
        "+13015550003": {"texts_blocked": 1},
        "+13065550004": {"calls_forwarded": 1, "country_code": "CA"},
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
        real_number = relay_number.replace("+1301", "+1201").replace("+1306", "+1639")
        verification_date = verification_base_date + timedelta(seconds=60 * num)
        country_code = data.pop("country_code", "US")
        baker.make(
            RealPhone,
            user=user,
            number=real_number,
            verification_sent_date=verification_date,
            verified=True,
            country_code=country_code,
        )
        in_relay = test_data.pop("in_relay", True)
        in_twilio = test_data.pop("in_twilio", True)
        if in_relay:
            baker.make(RelayNumber, user=user, number=relay_number, **data)
        if in_twilio:
            twilio_objects.append(create_mock_number_instance(relay_number, settings))
    mock_twilio_client.incoming_phone_numbers.list.return_value = twilio_objects


def create_mock_number_instance(phone_number: str, settings) -> Mock:
    """
    Create a mock IncomingPhoneNumberInstance.

    Omitted properties: address_requirements, address_sid, api_version, beta,
    bundle_sid, capabilities, date_created, date_updated,
    emergency_address_sid, emergency_address_status, emergency_status,
    friendly_name, origin, sms_fallback_method, sms_fallback_url, sms_method,
    sms_url, status, status_callback, status_callback_method, trunk_sid, uri,
    voice_caller_id_lookup, voice_fallback_method, voice_fallback_url,
    voice_method,voice_receive_mode, voice_url
    """
    return Mock(
        account_sid=settings.TWILIO_ACCOUNT_SID,
        phone_number=phone_number,
        sid=f"PN{uuid4().hex}",
        sms_application_sid=settings.TWILIO_SMS_APPLICATION_SID,
        voice_application_sid=settings.TWILIO_SMS_APPLICATION_SID,
    )


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
        "relay_numbers_by_country_code": {"all": 0},
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
Relay Numbers by Country Code:
  All: 0
Twilio Numbers:
  All: 0
Sync Check:
  All: 0"""
    assert report == expected


@pytest.mark.django_db
def test_relay_number_sync_checker_just_main(mock_twilio_client, settings) -> None:
    """RelayNumberSyncChecker works on an empty database."""
    mock_twilio_client.incoming_phone_numbers.list.return_value = [
        create_mock_number_instance(settings.TWILIO_MAIN_NUMBER, settings)
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
        "relay_numbers_by_country_code": {"all": 0},
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
Relay Numbers by Country Code:
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


def test_relay_number_sync_checker_synced_with_twilio(
    setup_relay_number_test_data,
) -> None:
    """RelayNumberSyncChecker detects that all phone numbers are synced."""
    checker = RelayNumberSyncChecker()
    assert checker.issues() == 0
    assert checker.counts == {
        "summary": {"ok": 7, "needs_cleaning": 0},
        "relay_numbers": {
            "all": 7,
            "disabled": 1,
            "enabled": 6,
            "used": 5,
            "used_calls": 2,
            "used_texts": 2,
            "used_both": 1,
        },
        "relay_numbers_by_country_code": {"all": 7, "CA": 1, "US": 6},
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
Relay Numbers by Country Code:
  All: 7
    CA: 1 (14.3%)
    US: 6 (85.7%)
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


@pytest.mark.relay_test_data(main_number_in_twilio=False)
def test_relay_number_sync_checker_main_not_in_twilio(
    setup_relay_number_test_data,
) -> None:
    """RelayNumberSyncChecker detects that all phone numbers are synced."""
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
        "relay_numbers_by_country_code": {"CA": 1, "US": 6, "all": 7},
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
Relay Numbers by Country Code:
  All: 7
    CA: 1 (14.3%)
    US: 6 (85.7%)
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
