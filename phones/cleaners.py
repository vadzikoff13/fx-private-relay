"""Tasks that detect data issues and (if possible) clean them."""

from __future__ import annotations
from collections import Counter

from django.conf import settings
from django.db.models import Q

from privaterelay.cleaners import (
    DetectorTask,
    CleanupData,
    Counts,
    SectionSpec,
    SubSectionSpec,
)

from .models import twilio_client, RelayNumber, TwilioMessagingService


class RelayNumberSyncChecker(DetectorTask):
    slug = "relay-numbers"
    title = "Check if the RelayNumber table is in sync with the Twilio numbers."
    check_description = (
        "The numbers in the `RelayNumber` table should be the same as Twilio's"
        " `IncomingPhoneNumber` resource."
    )

    def _get_counts_and_data(self) -> tuple[Counts, CleanupData]:
        """
        Analyze local RelayNumber table and compare to Twilio's data.

        Returns:
        * counts: two-level dict of row counts for RelayNumber and Twilio's
          IncomingPhoneNumber.
        * cleanup_data: two-element dict of RelayNumber entries to...
        """
        # Collect RelayNumber usage data
        relay_all = RelayNumber.objects.count()
        disabled = RelayNumber.objects.filter(enabled=False)
        enabled = RelayNumber.objects.filter(enabled=True)
        q_used_texts = Q(texts_forwarded__gt=0) | Q(texts_blocked__gt=0)
        q_used_calls = Q(calls_forwarded__gt=0) | Q(calls_blocked__gt=0)
        used = enabled.filter(q_used_texts | q_used_calls)
        used_both = enabled.filter(q_used_texts & q_used_calls)
        used_texts = enabled.filter(q_used_texts & ~q_used_calls)
        used_calls = enabled.filter(q_used_calls & ~q_used_texts)

        # Collect the TwilioMessagingService service IDs
        relay_service_ids = {
            service.id: service for service in TwilioMessagingService.objects.all()
        }
        relay_services = set(
            service.service_id for service in relay_service_ids.values()
        )

        # Collect all the RelayNumbers, their country code, and their service ID
        relay_data = [
            (
                number,
                country_code,
                relay_service_ids[service_id].service_id if service_id else None,
            )
            for number, country_code, service_id in RelayNumber.objects.values_list(
                "number", "country_code", "service_id"
            )
        ]
        relay_numbers = set(triple[0] for triple in relay_data)

        # Collect all the Twilio IncomingPhoneNumbers
        client = twilio_client()
        twilio_objs = client.incoming_phone_numbers.list()
        twilio_numbers: set[str] = set(obj.phone_number for obj in twilio_objs)

        # Count countries, with and without services, for the numbers in both databases
        in_both_db = relay_numbers & twilio_numbers
        country_code_with_service_id = Counter(
            (country_code, service_id is not None)
            for number, country_code, service_id in relay_data
            if number in in_both_db
        )
        country_codes = set(
            country for country, _ in country_code_with_service_id.keys()
        )

        # Gather numbers assigned to messaging services
        services = {
            service.sid: service for service in client.messaging.v1.services.list()
        }
        twilio_service_id_for_number: dict[str, str] = {}
        for service_id, service in services.items():
            twilio_service_id_for_number.update(
                {pn.phone_number: service_id for pn in service.phone_numbers.list()}
            )
        twilio_services = set(services.keys())

        # Account for the main number, in Twilio but not a RelayNumber
        main_number = settings.TWILIO_MAIN_NUMBER
        if main_number:
            main_in_twilio = main_number in twilio_numbers
            main_in_service = (
                main_in_twilio and main_number in twilio_service_id_for_number
            )
            main_set = set((main_number,))
        else:
            main_in_twilio = False
            main_in_service = False
            main_set = set()

        # Count numbers only in one database
        only_relay_db_count = len(relay_numbers - twilio_numbers)
        only_twilio_db_count = len(twilio_numbers - relay_numbers - main_set)

        # Count services only in one database
        service_in_both_db = relay_services & twilio_services
        service_only_relay_db = relay_services - twilio_services
        service_only_twilio_db = twilio_services - relay_services

        # Count RelayNumbers that are OK vs need cleaning
        needs_cleaning = only_relay_db_count + only_twilio_db_count
        ok = len(in_both_db)
        if main_in_twilio:
            ok += 1
        else:
            needs_cleaning += 1

        counts: Counts = {
            "summary": {
                "ok": ok,
                "needs_cleaning": needs_cleaning,
            },
            "relay_numbers": {
                "all": relay_all,
                "disabled": disabled.count(),
                "enabled": enabled.count(),
                "used": used.count(),
                "used_both": used_both.count(),
                "used_texts": used_texts.count(),
                "used_calls": used_calls.count(),
            },
            "twilio_numbers": {
                "all": len(relay_numbers | twilio_numbers),
                "in_both_db": len(in_both_db),
                "only_relay_db": only_relay_db_count,
                "only_twilio_db": only_twilio_db_count,
                "main_number": 1 if main_in_twilio else 0,
            },
            "twilio_messaging_services": {
                "all": len(relay_services | twilio_services),
                "in_both_db": len(service_in_both_db),
                "only_relay_db": len(service_only_relay_db),
                "only_twilio_db": len(service_only_twilio_db),
            },
        }
        if main_in_twilio:
            counts["twilio_numbers"]["main_number_in_service"] = (
                1 if main_in_service else 0
            )
            counts["twilio_numbers"]["main_number_no_service"] = (
                0 if main_in_service else 1
            )

        for code in country_codes:
            # TODO: check number is in Django and Twilio service
            in_service = country_code_with_service_id[code, True]
            no_service = country_code_with_service_id[code, False]
            counts["twilio_numbers"][f"cc_{code}"] = in_service + no_service
            counts["twilio_numbers"][f"cc_{code}_in_service"] = in_service
            counts["twilio_numbers"][f"cc_{code}_no_service"] = no_service

        cleanup_data: CleanupData = {}
        return counts, cleanup_data

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
                - Not in a Messaging Service
              - US
                - In a Messaging Service
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
                    has_service = SubSectionSpec(
                        "In a Messaging Service", key=f"{key}_in_service"
                    )
                    no_service = SubSectionSpec(
                        "Not in a Messaging Service", key=f"{key}_no_service"
                    )
                    in_both.subsections.append(
                        SubSectionSpec(
                            f"Country Code {key.removeprefix('cc_')}",
                            key=key,
                            subsections=[has_service, no_service],
                        )
                    )

        return [
            SectionSpec("Relay Numbers", subsections=[relay_all]),
            SectionSpec("Twilio Numbers", subsections=[twilio_all]),
            SectionSpec("Twilio Messaging Services", subsections=[service_all]),
        ]
