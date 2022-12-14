"""Tasks that detect data issues and (if possible) clean them."""

from __future__ import annotations
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from django.conf import settings
from django.db.models import Q
from django.utils.functional import cached_property

from privaterelay.cleaners import (
    DetectorTask,
    CleanupData,
    Counts,
    SectionSpec,
    SubSectionSpec,
)

from .models import twilio_client, RelayNumber, TwilioMessagingService


@dataclass
class _RelayNumberData:
    """RelayNumber data from the Relay database."""

    number: str
    country_code: str
    service_id: Optional[str]


@dataclass
class _RelayServiceData:
    service_id: str
    friendly_name: str
    channel: str
    spam: bool
    full: bool
    campaign_use_case: str
    campaign_status: str


@dataclass
class _TwilioServiceData:
    """Messaging Service data from Twilio."""

    service_id: str
    numbers: list[str]


@dataclass
class _CombinedNumber:
    """Combined data for a number from RelayNumber and Twilio."""

    number: str
    country_code: Optional[str] = None
    is_main_number: bool = False
    is_relaynumber: bool = False
    is_twilio_number: bool = False
    relay_service_id: Optional[str] = None
    twilio_service_id: Optional[str] = None

    @property
    def in_relay(self):
        """Return True if this number is a known Relay number."""
        return self.is_relaynumber or self.is_main_number

    @property
    def twilio_only(self):
        """Return True if this number is only in Twilio."""
        return self.is_twilio_number and not self.in_relay

    @property
    def is_synced(self):
        """Return True if Relay and Twilio are in sync for this number."""
        return (
            self.in_relay
            and self.is_twilio_number
            and (self.is_main_number or self.relay_service_id == self.twilio_service_id)
        )

    @property
    def has_service(self):
        """Return True if this number is assigned to a service"""
        return self.twilio_service_id is not None and (
            self.is_main_number or self.relay_service_id == self.twilio_service_id
        )

    @property
    def needs_service(self):
        """Return True if this number needs a Messaging Service"""
        return not self.has_service and (
            self.is_main_number or (self.is_relaynumber and self.country_code == "US")
        )

    @property
    def can_sync(self):
        """Return True if out of sync and it can be fixed automatically."""
        return (
            self.is_synced
            or self.in_relay
            or not (self.twilio_only or self.has_service)
        )

    @property
    def manual_sync(self):
        """Return True if out of sync and requires manual cleanup."""
        return self.is_synced or not self.is_twilio_number


class _CombinedNumberData:
    """Combined number data from RelayNumbers and Twilio."""

    def __init__(
        self,
        main_number: Optional[str],
        relaynumber_data: list[_RelayNumberData],
        twilio_numbers: list[str],
        twilio_services: list[_TwilioServiceData],
    ) -> None:
        self.main_number = main_number
        self._numbers: dict[str, _CombinedNumber] = {}
        self._can_sync: Optional[list[_CombinedNumber]] = None
        self._manual_sync: Optional[list[_CombinedNumber]] = None

        if main_number:
            self._numbers[main_number] = _CombinedNumber(
                number=main_number, is_main_number=True
            )

        for data in relaynumber_data:
            number = data.number
            assert number not in self._numbers
            self._numbers[number] = _CombinedNumber(
                number=number,
                country_code=data.country_code,
                is_relaynumber=True,
                relay_service_id=data.service_id,
            )

        for number in twilio_numbers:
            if number in self._numbers:
                self._numbers[number].is_twilio_number = True
            else:
                self._numbers[number] = _CombinedNumber(
                    number=number, is_twilio_number=True
                )

        for service_data in twilio_services:
            for number in service_data.numbers:
                try:
                    self._numbers[number].twilio_service_id = service_data.service_id
                except KeyError:
                    pass

    @cached_property
    def main_in_twilio(self) -> bool:
        """Return true if the main number is registered with Twilio."""
        return (
            self.main_number is not None
            and self._numbers[self.main_number].is_twilio_number
        )

    @cached_property
    def main_in_service(self) -> bool:
        """Return true if the main number is in a Twilio Messaging Service."""
        return (
            self.main_number is not None
            and self._numbers[self.main_number].twilio_service_id is not None
        )

    @cached_property
    def _count_by_presence(self) -> dict[tuple[bool, bool], int]:
        """
        Return count by presence in RelayNumbers table and Twilio.

        Return a Counter (dict) with key (is in Relay?, is in Twilio?)
        """

        return Counter(
            (num.is_relaynumber, num.is_twilio_number)
            for num in self._numbers.values()
            if not num.is_main_number
        )

    @property
    def all_count(self) -> int:
        """Return count of all numbers."""
        return sum(self._count_by_presence.values()) + (1 if self.main_in_twilio else 0)

    @property
    def in_both_db_count(self) -> int:
        """Return count of numbers in both Relay and Twilio."""
        return self._count_by_presence[True, True]

    @property
    def only_relay_db_count(self) -> int:
        """Return count of numbers only in Relay database."""
        return self._count_by_presence[True, False]

    @property
    def only_twilio_db_count(self) -> int:
        """Return count of numbers only in Twilio database."""
        return self._count_by_presence[False, True]

    @lru_cache
    def _sync_counts(
        self,
    ) -> tuple[dict[bool, int], list[_CombinedNumber], list[_CombinedNumber]]:
        """
        Return count of data, and items to sync

        Return is a tuple:
        - Counter (dict) with key (needs sync?)
        - List of numbers that can be automatically synced
        - List of numbers that need to be manually synced
        """
        needs_sync: dict[bool, int] = Counter()
        can_sync: list[_CombinedNumber] = []
        manual_sync: list[_CombinedNumber] = []

        for number in self._numbers.values():
            if number.is_synced and not number.needs_service:
                needs_sync[False] += 1
            elif number.can_sync:
                needs_sync[True] += 1
                can_sync.append(number)
            elif number.manual_sync:
                needs_sync[True] += 1
                manual_sync.append(number)
        return needs_sync, can_sync, manual_sync

    @property
    def ok(self):
        """Return count of items that do not need syncing"""
        return self._sync_counts()[0][False]

    @property
    def needs_sync(self):
        """Return count of items that need syncing"""
        return self._sync_counts()[0][True]

    def get_cleanup_data(self) -> dict[str, list[_CombinedNumber]]:
        _, can_sync, manual_sync = self._sync_counts()
        return {
            "numbers_to_sync": can_sync,
            "numbers_to_manually_sync": manual_sync,
        }

    @property
    def country_codes(self) -> list[str]:
        """Return country codes, alphabetical order."""
        return sorted(
            set(
                number.country_code
                for number in self._numbers.values()
                if number.country_code
            )
        )

    @lru_cache
    def _service_count_by_country(
        self, country_code=str
    ) -> dict[tuple[bool, bool], int]:
        """
        Return count by messaging service assignment in Relay and Twilio.

        Return a Counter (dict) with key (is in Relay Service?, is in Twilio Service?)
        """
        return Counter(
            (bool(num.relay_service_id), bool(num.twilio_service_id))
            for num in self._numbers.values()
            if not num.is_main_number and num.country_code == country_code
        )

    def country_count(self, country_code: str) -> int:
        """Return count of all numbers with this country_code."""
        return sum(self._service_count_by_country(country_code).values())

    def in_both_services_count(self, country_code: str) -> int:
        """Return count of country's numbers assigned to service on both sides."""
        return self._service_count_by_country(country_code)[True, True]

    def only_relay_service_count(self, country_code: str) -> int:
        """Return count of country's numbers assigned only to a Relay service."""
        return self._service_count_by_country(country_code)[True, False]

    def only_twilio_service_count(self, country_code: str) -> int:
        """Return count of country's numbers assigned only in Twilio database."""
        return self._service_count_by_country(country_code)[False, True]

    def no_service_count(self, country_code: str) -> int:
        """Return count of country's numbers assigned to no service."""
        return self._service_count_by_country(country_code)[False, False]


@dataclass
class _CombinedService:
    """Combined service data from Relay TwilioMessagingService and Twilio."""

    service_id: str
    is_relay_service: bool = False
    is_twilio_service: bool = False


class _CombinedServiceData:
    """Combined service data from Relay and Twilio."""

    def __init__(
        self,
        relay_services: list[_RelayServiceData],
        twilio_services: list[_TwilioServiceData],
    ) -> None:
        self._services: dict[str, _CombinedService] = {}

        for relay_service in relay_services:
            service_id = relay_service.service_id
            assert service_id not in self._services
            self._services[service_id] = _CombinedService(
                service_id=service_id,
                is_relay_service=True,
            )

        for twilio_service in twilio_services:
            service_id = twilio_service.service_id
            if service_id in self._services:
                self._services[service_id].is_twilio_service = True
            else:
                self._services[service_id] = _CombinedService(
                    service_id=service_id,
                    is_twilio_service=True,
                )

    @cached_property
    def _count_by_presence(self) -> dict[tuple[bool, bool], int]:
        """
        Return count by presence in TwilioMessagingService table and Twilio.

        Return a Counter (dict) with key (is in Relay?, is in Twilio?)
        """

        return Counter(
            (service.is_relay_service, service.is_twilio_service)
            for service in self._services.values()
        )

    @property
    def all_count(self) -> int:
        """Return count of all numbers."""
        return sum(self._count_by_presence.values())

    @property
    def in_both_db_count(self) -> int:
        """Return count of numbers in both Relay and Twilio."""
        return self._count_by_presence[True, True]

    @property
    def only_relay_db_count(self) -> int:
        """Return count of numbers only in Relay database."""
        return self._count_by_presence[True, False]

    @property
    def only_twilio_db_count(self) -> int:
        """Return count of numbers only in Twilio database."""
        return self._count_by_presence[False, True]


class RelayNumberSyncChecker(DetectorTask):
    slug = "relay-numbers"
    title = "Check if the RelayNumber table is in sync with the Twilio numbers."
    check_description = (
        "The numbers in the `RelayNumber` table should be in the Twilio"
        " `IncomingPhoneNumber` resource, with the main number and US numbers"
        " assigned to a Twilio Messaging Service."
    )

    def _get_counts_and_data(self) -> tuple[Counts, CleanupData]:
        """
        Analyze local RelayNumber table and compare to Twilio's data.

        Returns:
        * counts: two-level dict of row counts for RelayNumber, Twilio's
          IncomingPhoneNumber, and Twilio's Messaging Service
        * cleanup_data: two-element dict of RelayNumber entries to...
        """
        # Gather data from Relay database
        relay_number_data = self._relay_number_data()
        twilio_messaging_service_data = self._twilio_messaging_service_data()

        # Gather data from Twilio database
        client = twilio_client()
        twilio_numbers = self._twilio_numbers(client)
        twilio_services = self._twilio_services(client)

        # Combine the two sources
        number_data = _CombinedNumberData(
            main_number=settings.TWILIO_MAIN_NUMBER,
            relaynumber_data=relay_number_data,
            twilio_numbers=twilio_numbers,
            twilio_services=twilio_services,
        )
        service_data = _CombinedServiceData(
            relay_services=twilio_messaging_service_data,
            twilio_services=twilio_services,
        )

        # Gather initial counts
        counts: Counts = {
            "summary": {
                "ok": number_data.ok,
                "needs_cleaning": number_data.needs_sync,
            },
            "relay_numbers": self._relaynumber_usage_counts(),
            "twilio_numbers": {
                "all": number_data.all_count,
                "in_both_db": number_data.in_both_db_count,
                "only_relay_db": number_data.only_relay_db_count,
                "only_twilio_db": number_data.only_twilio_db_count,
                "main_number": 1 if number_data.main_in_twilio else 0,
            },
            "twilio_messaging_services": {
                "all": service_data.all_count,
                "in_both_db": service_data.in_both_db_count,
                "only_relay_db": service_data.only_relay_db_count,
                "only_twilio_db": service_data.only_twilio_db_count,
            },
        }

        # Handle main number
        if number_data.main_in_twilio:
            counts["twilio_numbers"].update(
                {
                    "main_number": 1,
                    "main_number_in_service": 1 if number_data.main_in_service else 0,
                    "main_number_no_service": 0 if number_data.main_in_service else 1,
                }
            )
        else:
            counts["twilio_numbers"]["main_number"] = 0

        # Gather per-country counts
        for code in number_data.country_codes:
            counts["twilio_numbers"][f"cc_{code}"] = number_data.country_count(code)
            counts["twilio_numbers"][
                f"cc_{code}_in_service"
            ] = number_data.in_both_services_count(code)
            counts["twilio_numbers"][
                f"cc_{code}_no_service"
            ] = number_data.no_service_count(code)
            counts["twilio_numbers"][
                f"cc_{code}_only_relay_service"
            ] = number_data.only_relay_service_count(code)
            counts["twilio_numbers"][
                f"cc_{code}_only_twilio_service"
            ] = number_data.only_twilio_service_count(code)

        cleanup_data: CleanupData = number_data.get_cleanup_data()
        return counts, cleanup_data

    def _relaynumber_usage_counts(self) -> dict[str, int]:
        """
        Get usage data for RelayNumbers

        This is for staff interest, no fixes needed.
        """
        # Setup queries for RelayNumber usage data
        relay_all_count = RelayNumber.objects.count()
        disabled = RelayNumber.objects.filter(enabled=False)
        enabled = RelayNumber.objects.filter(enabled=True)
        q_used_texts = Q(texts_forwarded__gt=0) | Q(texts_blocked__gt=0)
        q_used_calls = Q(calls_forwarded__gt=0) | Q(calls_blocked__gt=0)
        used = enabled.filter(q_used_texts | q_used_calls)
        used_both = enabled.filter(q_used_texts & q_used_calls)
        used_texts = enabled.filter(q_used_texts & ~q_used_calls)
        used_calls = enabled.filter(q_used_calls & ~q_used_texts)

        # Return counts
        return {
            "all": relay_all_count,
            "disabled": disabled.count(),
            "enabled": enabled.count(),
            "used": used.count(),
            "used_both": used_both.count(),
            "used_texts": used_texts.count(),
            "used_calls": used_calls.count(),
        }

    def _relay_number_data(self) -> list[_RelayNumberData]:
        """Get Relay's data for numbers and related service."""
        return list(
            _RelayNumberData(*vals)
            for vals in RelayNumber.objects.values_list(
                "number", "country_code", "service__service_id"
            )
        )

    def _twilio_messaging_service_data(self) -> list[_RelayServiceData]:
        """Get Relay's data for the Twilio Messaging Service."""
        return list(
            _RelayServiceData(*vals)
            for vals in TwilioMessagingService.objects.values_list(
                "service_id",
                "friendly_name",
                "channel",
                "spam",
                "full",
                "campaign_use_case",
                "campaign_status",
            )
        )

    def _twilio_numbers(self, client) -> list[str]:
        """Get Twilio's number data."""
        return [obj.phone_number for obj in client.incoming_phone_numbers.list()]

    def _twilio_services(self, client) -> list[_TwilioServiceData]:
        """Get Twilio's service data."""
        data = []
        for service in client.messaging.v1.services.list():
            service_id = service.sid
            numbers = [pn.phone_number for pn in service.phone_numbers.list()]
            data.append(_TwilioServiceData(service_id, numbers))
        return data

    def markdown_report_spec(self) -> list[SectionSpec]:
        """
        Return specification for RelayNumberCleaner.

        - Relay Numbers
          - All
            - Enabled
              - Used
                - Used for Texts Only
                - Used for Calls Only
                - Used for Both
        - Twilio Numbers
          - All
            - In Both Databases
              - CA
                - In a Messaging Service
                - Only in Relay Service Table
                - Only in Twilio Service
                - Not in a Messaging Service
              - US
                - In a Messaging Service
                - Only in Relay Service Table
                - Only in Twilio Service
                - Not in a Messaging Service
            - Main Number in Twilio Database
              - In a Messaging Service
              - Not in a Messaging Service
            - Only in Relay Database
            - Only in Twilio Database
        - Twilo Messaging Services
          - All
            - In Both Databases
            - Only in Relay Database
            - Only in Twilio Database
        """
        relay_all = SubSectionSpec("All", is_total_count=True)
        relay_enabled = SubSectionSpec("Enabled")
        relay_used = SubSectionSpec("Used")
        relay_used_texts = SubSectionSpec("Used for Texts Only", key="used_texts")
        relay_used_calls = SubSectionSpec("Used for Calls Only", key="used_calls")
        relay_used_both = SubSectionSpec("Used for Both", key="used_both")
        relay_all.subsections = [relay_enabled]
        relay_enabled.subsections = [relay_used]
        relay_used.subsections = [relay_used_texts, relay_used_calls, relay_used_both]

        twilio_all = SubSectionSpec("All", is_total_count=True)
        in_both = SubSectionSpec("In Both Databases", key="in_both_db")
        only_relay = SubSectionSpec("Only in Relay Database", key="only_relay_db")
        only_twilio = SubSectionSpec("Only in Twilio Database", key="only_twilio_db")
        main_number = SubSectionSpec("Main Number in Twilio", key="main_number")
        main_in_service = SubSectionSpec(
            "In a Messaging Service", key="main_number_in_service"
        )
        main_no_service = SubSectionSpec(
            "Not in a Messaging Service", key="main_number_no_service"
        )
        twilio_all.subsections = [in_both, main_number, only_relay, only_twilio]
        main_number.subsections = [main_in_service, main_no_service]

        service_all = SubSectionSpec(
            "All", is_total_count=True, subsections=[in_both, only_relay, only_twilio]
        )

        # Dynamically add the country code subsections
        if self._counts:
            for key in sorted(self._counts["twilio_numbers"]):
                if key.startswith("cc_") and not key.endswith("_service"):
                    name_and_key_suffix = (
                        ("In a Messaging Service", "_in_service"),
                        ("Only in Relay Service Table", "_only_relay_service"),
                        ("Only in Twilio Service", "_only_twilio_service"),
                        ("Not in a Messaging Service", "_no_service"),
                    )
                    subsections = [
                        SubSectionSpec(name, key=key + suffix)
                        for name, suffix in name_and_key_suffix
                    ]
                    in_both.subsections.append(
                        SubSectionSpec(
                            f"Country Code {key.removeprefix('cc_')}",
                            key=key,
                            subsections=subsections,
                        )
                    )

        return [
            SectionSpec("Relay Numbers", subsections=[relay_all]),
            SectionSpec("Twilio Numbers", subsections=[twilio_all]),
            SectionSpec("Twilio Messaging Services", subsections=[service_all]),
        ]
