"""Tasks that detect data issues and (if possible) clean them."""

from __future__ import annotations

from django.conf import settings
from django.db.models import Count, Q

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
        "The numbers in the RelayNumber table should be the same as Twilio's"
        " IncomingPhoneNumber resource."
    )

    def _get_counts_and_data(self) -> tuple[Counts, CleanupData]:
        """
        Analyze local RelayNumber table and compare to Twilio's data.

        Returns:
        * counts: two-level dict of row counts for RelayNumber and Twilio's
          IncomingPhoneNumber.
        * cleanup_data: two-element dict of RelayNumber entries to...
        """
        disabled = RelayNumber.objects.filter(enabled=False)
        enabled = RelayNumber.objects.filter(enabled=True)
        q_used_texts = Q(texts_forwarded__gt=0) | Q(texts_blocked__gt=0)
        q_used_calls = Q(calls_forwarded__gt=0) | Q(calls_blocked__gt=0)
        used = enabled.filter(q_used_texts | q_used_calls)
        used_both = enabled.filter(q_used_texts & q_used_calls)
        used_texts = enabled.filter(q_used_texts & ~q_used_calls)
        used_calls = enabled.filter(q_used_calls & ~q_used_texts)

        relay_numbers = set(RelayNumber.objects.values_list("number", flat=True))

        client = twilio_client()
        twilio_objs = client.incoming_phone_numbers.list()
        twilio_numbers: set[str] = set(obj.phone_number for obj in twilio_objs)

        if settings.TWILIO_MAIN_NUMBER:
            main_in_twilio = settings.TWILIO_MAIN_NUMBER in twilio_numbers
            main_set = set((settings.TWILIO_MAIN_NUMBER,))
        else:
            main_in_twilio = False
            main_set = set()

        only_relay_db = len(relay_numbers - twilio_numbers)
        only_twilio_db = len(twilio_numbers - relay_numbers - main_set)
        needs_cleaning = only_relay_db + only_twilio_db
        if not main_in_twilio:
            needs_cleaning += 1

        counts: Counts = {
            "summary": {
                "ok": RelayNumber.objects.count(),
                "needs_cleaning": needs_cleaning,
            },
            "relay_numbers": {
                "all": RelayNumber.objects.count(),
                "disabled": disabled.count(),
                "enabled": enabled.count(),
                "used": used.count(),
                "used_both": used_both.count(),
                "used_texts": used_texts.count(),
                "used_calls": used_calls.count(),
            },
            "twilio_numbers": {
                "all": len(twilio_numbers),
                "main_number": 1 if main_in_twilio else 0,
            },
            "sync_check": {
                "all": len(relay_numbers | twilio_numbers),
                "in_both_db": len(relay_numbers & twilio_numbers),
                "main_number": 1 if main_in_twilio else 0,
                "only_relay_db": only_relay_db,
                "only_twilio_db": only_twilio_db,
            },
        }
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
            - Main Number
        - Sync Check
          - All
            - In Both Databases
            - Main Number in Twilio Database
            - Only in Relay Database
            - Only in Twilio Database
        """
        relay_all = SubSectionSpec("All", is_total_count=True)
        twilio_all = SubSectionSpec("All", is_total_count=True)
        sync_all = SubSectionSpec("All", is_total_count=True)
        relay_enabled = SubSectionSpec("Enabled")
        relay_used = SubSectionSpec("Used")
        relay_used_texts = SubSectionSpec("Used for Texts Only", key="used_texts")
        relay_used_calls = SubSectionSpec("Used for Calls Only", key="used_calls")
        relay_used_both = SubSectionSpec("Used for Both", key="used_both")
        main_number = SubSectionSpec("Main Number")
        in_both = SubSectionSpec("In Both Databases", key="in_both_db")
        only_relay = SubSectionSpec("Only in Relay Database", key="only_relay_db")
        only_twilio = SubSectionSpec("Only in Twilio Database", key="only_twilio_db")
        main_number_sync = SubSectionSpec("Main Number in Twilio", key="main_number")

        relay_all.subsections = [relay_enabled]
        relay_enabled.subsections = [relay_used]
        relay_used.subsections = [relay_used_texts, relay_used_calls, relay_used_both]
        twilio_all.subsections = [main_number]
        sync_all.subsections = [in_both, main_number_sync, only_relay, only_twilio]
        return [
            SectionSpec("Relay Numbers", subsections=[relay_all]),
            SectionSpec("Twilio Numbers", subsections=[twilio_all]),
            SectionSpec("Sync Check", key="sync_check", subsections=[sync_all]),
        ]
