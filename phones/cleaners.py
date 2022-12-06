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

from .models import twilio_client, RelayNumber


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

        # Collect all the RelayNumbers and their country code
        country_code = {
            number: country_code
            for number, country_code in RelayNumber.objects.values_list(
                "number", "country_code"
            )
        }
        relay_numbers = set(country_code.keys())

        # Collect all the Twilio IncomingPhoneNumbers
        client = twilio_client()
        twilio_objs = client.incoming_phone_numbers.list()
        twilio_numbers: set[str] = set(obj.phone_number for obj in twilio_objs)

        # Count countries for the numbers in both databases
        in_both_db = relay_numbers & twilio_numbers
        country_code_counts = Counter((country_code[number] for number in in_both_db))

        # Account for the main number, in Twilio but not a RelayNumber
        if settings.TWILIO_MAIN_NUMBER:
            main_in_twilio = settings.TWILIO_MAIN_NUMBER in twilio_numbers
            main_set = set((settings.TWILIO_MAIN_NUMBER,))
        else:
            main_in_twilio = False
            main_set = set()

        # Account for numbers only in one database
        only_relay_db = len(relay_numbers - twilio_numbers)
        only_twilio_db = len(twilio_numbers - relay_numbers - main_set)
        needs_cleaning = only_relay_db + only_twilio_db
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
                "main_number": 1 if main_in_twilio else 0,
                "only_relay_db": only_relay_db,
                "only_twilio_db": only_twilio_db,
            },
        }
        for code, count in country_code_counts.items():
            counts["twilio_numbers"][f"cc_{code}"] = count

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
              - US
            - Main Number in Twilio Database
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
        twilio_all.subsections = [in_both, main_number, only_relay, only_twilio]

        # Dynamically add the country code subsections
        if self._counts:
            for key in sorted(self._counts["twilio_numbers"]):
                if key.startswith("cc_"):
                    code = key.removeprefix("cc_")
                    in_both.subsections.append(
                        SubSectionSpec(f"Country Code {code}", key=key)
                    )

        return [
            SectionSpec("Relay Numbers", subsections=[relay_all]),
            SectionSpec("Twilio Numbers", subsections=[twilio_all]),
        ]
