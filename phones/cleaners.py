"""Tasks that detect data issues and (if possible) clean them."""

from __future__ import annotations
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from django.conf import settings
from django.db.models import Q
from django.utils.functional import cached_property

from twilio.rest import Client

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
    service_channel: Optional[str]


@dataclass
class _RelayServiceData:
    service_id: str
    friendly_name: str
    use_case: str
    campaign_use_case: str
    campaign_status: str
    channel: str
    spam: bool
    size: int
    full: bool


@dataclass
class _TwilioCampaignData:
    """US A2P Campaign data from Twilio."""

    campaign_sid: str
    brand_registration_sid: str
    campaign_status: str
    us_app_to_person_usecase: str


class MultipleCampaignError(Exception):
    pass


@dataclass
class _TwilioServiceData:
    """Messaging Service data from Twilio."""

    service_id: str
    numbers: list[str]
    friendly_name: str
    status_callback: str
    us_app_to_person_registered: bool
    usecase: str
    use_inbound_webhook_on_number: bool
    campaigns: list[_TwilioCampaignData]

    @cached_property
    def campaign(self) -> Optional[_TwilioCampaignData]:
        """Return the campaign Twilio might be using."""
        if len(self.campaigns) == 1:
            return self.campaigns[0]
        if len(self.campaigns) == 0:
            return None
        raise MultipleCampaignError(
            "Messaging service has multiple campaigns.", self.campaigns
        )


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
    relay_service_channel: Optional[str] = None

    @property
    def in_relay(self) -> bool:
        """Return True if this number is a known Relay number."""
        return self.is_relaynumber or self.is_main_number

    @property
    def twilio_only(self) -> bool:
        """Return True if this number is only in Twilio."""
        return self.is_twilio_number and not self.in_relay

    @property
    def is_synced(self) -> bool:
        """Return True if Relay and Twilio are in sync for this number."""
        return (
            self.in_relay
            and self.is_twilio_number
            and (self.is_main_number or self.relay_service_id == self.twilio_service_id)
        )

    @property
    def has_service(self) -> bool:
        """Return True if this number is assigned to a service"""
        return self.twilio_service_id is not None and (
            self.is_main_number or self.relay_service_id == self.twilio_service_id
        )

    @property
    def needs_service(self) -> bool:
        """Return True if this number needs a Messaging Service"""
        return not self.has_service and (
            self.is_main_number or (self.is_relaynumber and self.country_code == "US")
        )

    def has_correct_service(self, relay_channel: str, main_channel: str) -> bool:
        """
        Returns True if this number is assigned to the correct service, or is
        unassigned and does not need to be assigned.
        """
        return (
            self.has_service
            and (
                (self.is_main_number and self.relay_service_channel == main_channel)
                or (
                    not self.is_main_number
                    and self.relay_service_channel == relay_channel
                )
            )
        ) or (self.is_relaynumber and self.country_code != "US")

    @property
    def can_sync(self) -> bool:
        """Return True if out of sync and it can be fixed automatically."""
        return (
            self.is_synced
            or self.in_relay
            or not (self.twilio_only or self.has_service)
        )

    @property
    def manual_sync(self) -> bool:
        """Return True if out of sync and requires manual cleanup."""
        return self.is_synced or not self.is_twilio_number


class _CombinedNumberData:
    """Combined number data from RelayNumbers and Twilio."""

    def __init__(
        self,
        main_number: Optional[str],
        relaynumber_data: list[_RelayNumberData],
        relay_services: list[_RelayServiceData],
        twilio_numbers: list[str],
        twilio_services: list[_TwilioServiceData],
        relay_service_channel: str,
        main_service_channel: str,
    ) -> None:
        self.main_number = main_number
        self.relay_service_channel = relay_service_channel
        self.main_service_channel = main_service_channel
        self._numbers: dict[str, _CombinedNumber] = {}
        self._can_sync: Optional[list[_CombinedNumber]] = None
        self._manual_sync: Optional[list[_CombinedNumber]] = None

        main_number_data = None
        if main_number:
            self._numbers[main_number] = main_number_data = _CombinedNumber(
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
                relay_service_channel=data.service_channel,
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

        # Set relay service info for main number
        if main_number_data and main_number_data.twilio_service_id:
            for service in relay_services:
                if service.service_id == main_number_data.twilio_service_id:
                    main_number_data.relay_service_id = service.service_id
                    main_number_data.relay_service_channel = service.channel

    @cached_property
    def main_in_twilio(self) -> bool:
        """Return True if the main number is registered with Twilio."""
        return (
            self.main_number is not None
            and self._numbers[self.main_number].is_twilio_number
        )

    @cached_property
    def main_in_service(self) -> bool:
        """Return True if the main number is in a Twilio Messaging Service."""
        return (
            self.main_number is not None
            and self._numbers[self.main_number].twilio_service_id is not None
        )

    @cached_property
    def main_in_correct_service(self) -> bool:
        """Return True if the main number is in TWILIO_MAIN_NUMBER_CHANNEL."""
        return (
            self.main_number is not None
            and self._numbers[self.main_number].twilio_service_id is not None
            and self._numbers[self.main_number].has_correct_service(
                relay_channel=self.relay_service_channel,
                main_channel=self.main_service_channel,
            )
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
            if number.is_synced and number.has_correct_service(
                self.relay_service_channel, self.main_service_channel
            ):
                needs_sync[False] += 1
            elif number.can_sync:
                needs_sync[True] += 1
                can_sync.append(number)
            elif number.manual_sync:
                needs_sync[True] += 1
                manual_sync.append(number)
        return needs_sync, can_sync, manual_sync

    @property
    def ok(self) -> int:
        """Return count of items that do not need syncing"""
        return self._sync_counts()[0][False]

    @property
    def needs_sync(self) -> int:
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
        self, country_code: str
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

    @lru_cache
    def _count_by_country_and_correct_service(
        self, country_code: str
    ) -> dict[bool, int]:
        """
        Return count of service-assigned numbers.

        Return a Counter (dict) with key (is in correct service?).
        Total should be same as in_both_services_count(
        """
        return Counter(
            num.has_correct_service(
                self.relay_service_channel, self.main_service_channel
            )
            for num in self._numbers.values()
            if not num.is_main_number
            and num.country_code == country_code
            and num.has_service
        )

    def in_correct_services_count(self, country_code: str) -> int:
        return self._count_by_country_and_correct_service(country_code)[True]

    def in_wrong_services_count(self, country_code: str) -> int:
        return self._count_by_country_and_correct_service(country_code)[False]


@dataclass
class _CombinedService:
    """Combined service data from Relay TwilioMessagingService and Twilio."""

    service_id: str
    is_relay_service: bool = False
    relay_friendly_name: Optional[str] = None
    relay_use_case: Optional[str] = None
    relay_campaign_use_case: Optional[str] = None
    relay_campaign_status: Optional[str] = None
    relay_channel: Optional[str] = None
    relay_spam: Optional[bool] = None
    relay_size: Optional[int] = None
    relay_full: Optional[bool] = None
    is_relay_service_channel: Optional[bool] = None
    is_main_service_channel: Optional[bool] = None
    is_twilio_service: bool = False
    twilio_friendly_name: Optional[str] = None
    twilio_status_callback: Optional[str] = None
    twilio_us_app_to_person_registered: Optional[bool] = None
    twilio_usecase: Optional[str] = None
    twilio_use_inbound_webhook_on_number: Optional[bool] = None
    twilio_campaign_sid: Optional[str] = None
    twilio_campaign_us_app_to_person_usecase: Optional[str] = None
    twilio_campaign_status: Optional[str] = None

    @property
    def in_both_db(self) -> bool:
        return self.is_relay_service and self.is_twilio_service

    @property
    def is_synced(self) -> bool:
        return (
            self.is_relay_service
            and self.is_twilio_service
            and self.relay_friendly_name == self.twilio_friendly_name
            and self.relay_use_case == self.twilio_usecase
            and self.relay_campaign_use_case
            == self.twilio_campaign_us_app_to_person_usecase
            and self.relay_campaign_status == self.twilio_campaign_status
        )

    @property
    def has_good_data(self) -> bool:
        """Return True if our channels have good data."""
        if not (self.is_relay_service_channel or self.is_main_service_channel):
            return True  # Unknown Relay channels are assumed valid

        if self.is_main_service_channel:
            expected_ua2p_usecase = "ACCOUNT_NOTIFICATION"
        else:
            expected_ua2p_usecase = "PROXY"
        return (
            self.twilio_status_callback is None
            and self.twilio_us_app_to_person_registered is True
            and self.twilio_usecase == "notifications"
            and self.twilio_use_inbound_webhook_on_number is True
            and self.twilio_campaign_us_app_to_person_usecase == expected_ua2p_usecase
        )

    @property
    def can_fix_data(self) -> bool:
        """Return True if our channel data can be fixed."""
        if not (self.is_relay_service_channel or self.is_main_service_channel):
            return False  # Can't fix a channel that isn't ours
        if self.is_main_service_channel:
            expected_ua2p_usecase = "ACCOUNT_NOTIFICATION"
        else:
            expected_ua2p_usecase = "PROXY"
        # Can fix anything but wrong US App To Person campaign
        return (
            self.twilio_us_app_to_person_registered is False
            or self.twilio_campaign_us_app_to_person_usecase == expected_ua2p_usecase
        )

    @property
    def can_sync(self) -> bool:
        return (self.is_twilio_service and not self.is_relay_service) or (
            self.is_twilio_service
            and self.is_relay_service
            and not (
                self.relay_friendly_name == self.twilio_friendly_name
                and self.relay_use_case == self.twilio_usecase
                and self.relay_campaign_use_case
                == self.twilio_campaign_us_app_to_person_usecase
                and self.relay_campaign_status == self.twilio_campaign_status
            )
        )

    @property
    def manual_sync(self) -> bool:
        return (self.is_relay_service and not self.is_twilio_service) or (
            bool(self.is_relay_service_channel or self.is_main_service_channel)
            and not self.can_fix_data
        )


class _CombinedServiceData:
    """Combined service data from Relay and Twilio."""

    def __init__(
        self,
        relay_services: list[_RelayServiceData],
        twilio_services: list[_TwilioServiceData],
        relay_service_channel: str,
        main_service_channel: str,
    ) -> None:
        self.relay_service_channel = relay_service_channel
        self.main_service_channel = main_service_channel
        self._services: dict[str, _CombinedService] = {}

        for relay_service in relay_services:
            service_id = relay_service.service_id
            assert service_id not in self._services
            self._services[service_id] = _CombinedService(
                service_id=service_id,
                is_relay_service=True,
                relay_friendly_name=relay_service.friendly_name,
                relay_use_case=relay_service.use_case,
                relay_campaign_use_case=relay_service.campaign_use_case,
                relay_campaign_status=relay_service.campaign_status,
                relay_channel=relay_service.channel,
                relay_spam=relay_service.spam,
                relay_size=relay_service.size,
                relay_full=relay_service.full,
                is_relay_service_channel=relay_service.channel == relay_service_channel,
                is_main_service_channel=relay_service.channel == main_service_channel,
            )

        for twilio_service in twilio_services:
            service_id = twilio_service.service_id
            if service_id in self._services:
                self._services[service_id].is_twilio_service = True
            else:
                self._services[service_id] = _CombinedService(
                    service_id=service_id, is_twilio_service=True
                )
            service = self._services[service_id]
            service.twilio_friendly_name = twilio_service.friendly_name
            service.twilio_status_callback = twilio_service.status_callback
            service.twilio_us_app_to_person_registered = (
                twilio_service.us_app_to_person_registered
            )
            service.twilio_usecase = twilio_service.usecase
            service.twilio_use_inbound_webhook_on_number = (
                twilio_service.use_inbound_webhook_on_number
            )
            if twilio_service.campaign:
                service.twilio_campaign_sid = twilio_service.campaign.campaign_sid
                service.twilio_campaign_us_app_to_person_usecase = (
                    twilio_service.campaign.us_app_to_person_usecase
                )
                service.twilio_campaign_status = twilio_service.campaign.campaign_status

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

    @lru_cache
    def _split_by_sync(
        self,
    ) -> tuple[list[_CombinedService], list[_CombinedService], list[_CombinedService]]:
        """
        Split services by sync status for further processing.

        Return is a tuple:
        - List of services in sync
        - List of services that can be automatically synced
        - List of services that need to be manually synced
        """
        synced: list[_CombinedService] = []
        can_sync: list[_CombinedService] = []
        manual_sync: list[_CombinedService] = []

        for service in self._services.values():
            if service.is_synced:
                synced.append(service)
            elif service.can_sync:
                can_sync.append(service)
            else:
                manual_sync.append(service)
        return synced, can_sync, manual_sync

    @property
    def needs_sync(self) -> int:
        """Return count of items that need syncing"""
        _, can_sync, manual_sync = self._split_by_sync()
        return len(can_sync) + len(manual_sync)

    @property
    def out_of_sync(self) -> int:
        """Return count of items that are in both databases but need syncing"""
        return len(
            list(
                service
                for service in self._services.values()
                if service.in_both_db and not service.is_synced
            )
        )

    @lru_cache
    def _split_by_data(self) -> tuple[list[_CombinedService], list[_CombinedService]]:
        """
        Split synced services by data status for further processing.

        Return is a tuple:
        - List of synced services with good data
        - List of synced services with bad data
        """
        good_data: list[_CombinedService] = []
        bad_data: list[_CombinedService] = []
        for service in self._split_by_sync()[0]:
            if service.has_good_data:
                good_data.append(service)
            else:
                bad_data.append(service)
        return good_data, bad_data

    @property
    def needs_fix(self) -> int:
        """Return count of items that need fixing"""
        _, bad_data = self._split_by_data()
        return len(bad_data)

    @property
    def synced_with_good_data_count(self) -> int:
        return len(self._split_by_data()[0])

    @property
    def ok(self) -> int:
        """Return count of items that do not need syncing or fixing"""
        return self.synced_with_good_data_count

    @property
    def synced_but_bad_data_count(self) -> int:
        return len(self._split_by_data()[1])

    @property
    def _count_by_readiness(self) -> dict[str, int]:
        """
        Count synced, good data services by readiness to use

        Return is a Counter with keys:
        * ready - ready for new numbers
        * campaign_pending - the 10 DLC campaign is pending review
        * not_ours - the service is for another Relay channel
        * spam - the service has been marked as spammy
        * full - the service has been marked as full
        """
        readiness: dict[str, int] = Counter()
        for service in self._split_by_data()[0]:
            if service.relay_full:
                readiness["full"] += 1
            elif service.relay_spam:
                readiness["spam"] += 1
            elif not (
                service.is_relay_service_channel or service.is_main_service_channel
            ):
                readiness["not_ours"] += 1
            elif service.relay_campaign_status == "PENDING":
                readiness["campaign_pending"] += 1
            elif service.relay_campaign_status != "VERIFIED":
                readiness["campaign_failed_or_other"] += 1
            else:
                readiness["ready"] += 1
        return readiness

    @property
    def ready_count(self) -> int:
        return self._count_by_readiness["ready"]

    @property
    def pending_count(self) -> int:
        return self._count_by_readiness["campaign_pending"]

    @property
    def failed_count(self) -> int:
        return self._count_by_readiness["campaign_failed_or_other"]

    @property
    def not_ours_count(self) -> int:
        return self._count_by_readiness["not_ours"]

    @property
    def spam_count(self) -> int:
        return self._count_by_readiness["spam"]

    @property
    def full_count(self) -> int:
        return self._count_by_readiness["full"]

    def get_cleanup_data(self) -> dict[str, list[_CombinedService]]:
        _, can_sync, manual_sync = self._split_by_sync()
        _, bad_data = self._split_by_data()
        return {
            "services_to_sync": can_sync,
            "services_to_manually_sync": manual_sync,
            "services_to_fix": bad_data,
        }


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
            relay_services=twilio_messaging_service_data,
            twilio_numbers=twilio_numbers,
            twilio_services=twilio_services,
            relay_service_channel=settings.TWILIO_CHANNEL,
            main_service_channel=settings.TWILIO_MAIN_NUMBER_CHANNEL,
        )
        service_data = _CombinedServiceData(
            relay_services=twilio_messaging_service_data,
            twilio_services=twilio_services,
            relay_service_channel=settings.TWILIO_CHANNEL,
            main_service_channel=settings.TWILIO_MAIN_NUMBER_CHANNEL,
        )

        # Gather initial counts
        counts: Counts = {
            "summary": {
                "ok": number_data.ok + service_data.ok,
                "needs_cleaning": number_data.needs_sync
                + service_data.needs_sync
                + service_data.needs_fix,
            },
            "relay_numbers": self._relaynumber_usage_counts(),
            "twilio_numbers": {
                "all": number_data.all_count,
                "in_both_db": number_data.in_both_db_count,
                "only_relay_db": number_data.only_relay_db_count,
                "only_twilio_db": number_data.only_twilio_db_count,
            },
            "twilio_messaging_services": {
                "all": service_data.all_count,
                "in_both_db": service_data.in_both_db_count,
                "only_relay_db": service_data.only_relay_db_count,
                "only_twilio_db": service_data.only_twilio_db_count,
            },
        }

        # Add main number
        if number_data.main_in_twilio:
            in_correct_service = number_data.main_in_correct_service
            in_wrong_service = number_data.main_in_service and not in_correct_service
            counts["twilio_numbers"].update(
                {
                    "main_number": 1,
                    "main_number_in_service": 1 if in_correct_service else 0,
                    "main_number_no_service": 0 if number_data.main_in_service else 1,
                    "main_number_wrong_service": 1 if in_wrong_service else 0,
                }
            )
        else:
            counts["twilio_numbers"]["main_number"] = 0

        # Add per-country counts, if any
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
            if number_data.in_both_services_count(code):
                counts["twilio_numbers"][
                    f"cc_{code}_correct_service"
                ] = number_data.in_correct_services_count(code)
                counts["twilio_numbers"][
                    f"cc_{code}_wrong_service"
                ] = number_data.in_wrong_services_count(code)

        # Add data about in-sync services, if any
        if service_data.in_both_db_count != 0:
            counts["twilio_messaging_services"].update(
                {
                    "synced_with_good_data": service_data.synced_with_good_data_count,
                    "synced_but_bad_data": service_data.synced_but_bad_data_count,
                    "out_of_sync": service_data.out_of_sync,
                }
            )
        if service_data.synced_with_good_data_count != 0:
            counts["twilio_messaging_services"].update(
                {
                    "ready": service_data.ready_count,
                    "pending": service_data.pending_count,
                    "failed": service_data.failed_count,
                    "not_ours": service_data.not_ours_count,
                    "spam": service_data.spam_count,
                    "full": service_data.full_count,
                }
            )

        cleanup_data: CleanupData = number_data.get_cleanup_data()
        cleanup_data.update(service_data.get_cleanup_data())
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
                "number",
                "country_code",
                "service__service_id",
                "service__channel",
            )
        )

    def _twilio_messaging_service_data(self) -> list[_RelayServiceData]:
        """Get Relay's data for the Twilio Messaging Service."""
        return list(
            _RelayServiceData(*vals)
            for vals in TwilioMessagingService.objects.values_list(
                "service_id",
                "friendly_name",
                "use_case",
                "campaign_use_case",
                "campaign_status",
                "channel",
                "spam",
                "size",
                "full",
            )
        )

    def _twilio_numbers(self, client: Client) -> list[str]:
        """Get Twilio's number data."""
        return [obj.phone_number for obj in client.incoming_phone_numbers.list()]

    def _twilio_services(self, client: Client) -> list[_TwilioServiceData]:
        """Get Twilio's service data."""
        data = []
        for service in client.messaging.v1.services.list():
            numbers = [pn.phone_number for pn in service.phone_numbers.list()]
            campaigns = [
                _TwilioCampaignData(
                    campaign_sid=campaign.sid,
                    brand_registration_sid=campaign.brand_registration_sid,
                    campaign_status=campaign.campaign_status,
                    us_app_to_person_usecase=campaign.us_app_to_person_usecase,
                )
                for campaign in service.us_app_to_person.list()
            ]

            service_data = _TwilioServiceData(
                service_id=service.sid,
                numbers=numbers,
                friendly_name=service.friendly_name,
                status_callback=service.status_callback,
                us_app_to_person_registered=service.us_app_to_person_registered,
                usecase=service.usecase,
                use_inbound_webhook_on_number=service.use_inbound_webhook_on_number,
                campaigns=campaigns,
            )
            data.append(service_data)
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
              - In Sync, Good Data
                - Ready to Use
                - Marked as Spam
                - Full
              - In Sync, Bad Data
              - Out of Sync
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
            "In Correct Messaging Service", key="main_number_in_service"
        )
        main_wrong_service = SubSectionSpec(
            "In Wrong Messaging Service", key="main_number_wrong_service"
        )
        main_no_service = SubSectionSpec(
            "Not in a Messaging Service", key="main_number_no_service"
        )
        twilio_all.subsections = [in_both, main_number, only_relay, only_twilio]
        main_number.subsections = [main_in_service, main_wrong_service, main_no_service]

        service_in_both = SubSectionSpec("In Both Databases", key="in_both_db")
        in_sync_good = SubSectionSpec("In Sync, Good Data", key="synced_with_good_data")
        in_sync_bad = SubSectionSpec("In Sync, Bad Data", key="synced_but_bad_data")
        out_of_sync = SubSectionSpec("Out of Sync")
        not_ours = SubSectionSpec("Not Ours", key="not_ours")
        ready = SubSectionSpec("Ready to Use", key="ready")
        in_progress = SubSectionSpec("Campaign Verification in Progress", key="pending")
        failed = SubSectionSpec("Campaign Failed or Unknown Status", key="failed")
        spam = SubSectionSpec("Marked as Spam", key="spam")
        full = SubSectionSpec("Full")

        service_all = SubSectionSpec("All", is_total_count=True)
        service_all.subsections = [service_in_both, only_relay, only_twilio]
        service_in_both.subsections = [in_sync_good, in_sync_bad, out_of_sync]
        in_sync_good.subsections = [ready, in_progress, failed, not_ours, spam, full]

        # Dynamically add the country code subsections
        counts = self.counts
        assert counts
        for key in sorted(counts["twilio_numbers"]):
            if key.startswith("cc_") and not key.endswith("_service"):
                country_code = key.removeprefix("cc_")
                name_and_key_suffix = (
                    ("In a Messaging Service", "_in_service"),
                    ("Only in Relay Messaging Service", "_only_relay_service"),
                    ("Only in Twilio Messaging Service", "_only_twilio_service"),
                    (
                        (
                            "Not in a Messaging Service"
                            f"{'' if country_code == 'US' else ' (OK)'}"
                        ),
                        "_no_service",
                    ),
                )

                subsections = [
                    SubSectionSpec(name, key=key + suffix)
                    for name, suffix in name_and_key_suffix
                ]
                subsections[0].subsections = [
                    SubSectionSpec("In Correct Service", key=f"{key}_correct_service"),
                    SubSectionSpec("In Wrong Service", key=f"{key}_wrong_service"),
                ]

                in_both.subsections.append(
                    SubSectionSpec(
                        f"Country Code {country_code}", key=key, subsections=subsections
                    )
                )

        return [
            SectionSpec("Relay Numbers", subsections=[relay_all]),
            SectionSpec("Twilio Numbers", subsections=[twilio_all]),
            SectionSpec("Twilio Messaging Services", subsections=[service_all]),
        ]
