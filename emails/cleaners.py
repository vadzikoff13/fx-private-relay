"""Tasks that detect data issues and (if possible) clean them."""

from __future__ import annotations

from django.contrib.auth.models import User
from django.db.models import Count, Q

from privaterelay.cleaners import (
    CleanerTask,
    CleanupData,
    Counts,
    SectionSpec,
    SubSectionSpec,
)

from .models import DomainAddress, Profile, RelayAddress
from .signals import create_user_profile


class ServerStorageCleaner(CleanerTask):
    slug = "server-storage"
    title = "Ensure no data is stored when server_storage=False"
    check_description = (
        "When Profile.server_storage is False, the addresses (both regular and domain)"
        " should have empty data (the fields description, generated_for and used_on)."
    )

    def _get_counts_and_data(self) -> tuple[Counts, CleanupData]:
        """
        Analyze usage of the server_storage flag and server-stored data.

        Returns:
        * counts: two-level dict of row counts for Profile, RelayAddress, and
          DomainAddress
        * cleanup_data: two-element dict of RelayAddresses and DomainAddress
          queries to clear
        """
        profiles_without_server_storage = Profile.objects.filter(server_storage=False)
        no_store_relay_addresses = RelayAddress.objects.filter(
            user__profile__server_storage=False
        )
        no_store_domain_addresses = DomainAddress.objects.filter(
            user__profile__server_storage=False
        )
        blank_used_on = Q(used_on="") | Q(used_on__isnull=True)
        blank_relay_data = blank_used_on & Q(description="") & Q(generated_for="")
        blank_domain_data = blank_used_on & Q(description="")

        empty_relay_addresses = no_store_relay_addresses.filter(blank_relay_data)
        empty_domain_addresses = no_store_domain_addresses.filter(blank_domain_data)
        non_empty_relay_addresses = no_store_relay_addresses.exclude(blank_relay_data)
        non_empty_domain_addresses = no_store_domain_addresses.exclude(
            blank_domain_data
        )

        empty_relay_addresses_count = empty_relay_addresses.count()
        empty_domain_addresses_count = empty_domain_addresses.count()
        non_empty_relay_addresses_count = non_empty_relay_addresses.count()
        non_empty_domain_addresses_count = non_empty_domain_addresses.count()

        counts: Counts = {
            "summary": {
                "ok": empty_relay_addresses_count + empty_domain_addresses_count,
                "needs_cleaning": non_empty_relay_addresses_count
                + non_empty_domain_addresses_count,
            },
            "profiles": {
                "all": Profile.objects.count(),
                "no_server_storage": profiles_without_server_storage.count(),
            },
            "relay_addresses": {
                "all": RelayAddress.objects.count(),
                "no_server_storage": no_store_relay_addresses.count(),
                "no_server_storage_or_data": empty_relay_addresses_count,
                "no_server_storage_but_data": non_empty_relay_addresses_count,
            },
            "domain_addresses": {
                "all": DomainAddress.objects.count(),
                "no_server_storage": no_store_domain_addresses.count(),
                "no_server_storage_or_data": empty_domain_addresses_count,
                "no_server_storage_but_data": non_empty_domain_addresses_count,
            },
        }
        cleanup_data: CleanupData = {
            "relay_addresses": non_empty_relay_addresses,
            "domain_addresses": non_empty_domain_addresses,
        }
        return counts, cleanup_data

    def _clean(self) -> int:
        """Clean addresses with unwanted server-stored data."""
        counts = self.counts
        cleanup_data = self.cleanup_data
        counts["relay_addresses"]["cleaned"] = cleanup_data["relay_addresses"].update(
            description="", generated_for="", used_on=""
        )
        counts["domain_addresses"]["cleaned"] = cleanup_data["domain_addresses"].update(
            description="", used_on=""
        )
        return (
            counts["relay_addresses"]["cleaned"] + counts["domain_addresses"]["cleaned"]
        )

    def markdown_report_spec(self) -> list[SectionSpec]:
        """
        Return specification for ServerStorageCleaner markdown report

        - Profiles
          - All
            - Without Server Storage
        - Relay Addresses
          - All
            - Without Server Storage
              - No Data
              - Has Data
                - Cleaned (if --clean)
        - Domain Addresses (same as Relay Addresses)
        """
        prof_all = SubSectionSpec("All", is_total_count=True)
        prof_no_storage = SubSectionSpec(
            "Without Server Storage", key="no_server_storage"
        )
        prof_all.subsections = [prof_no_storage]

        addr_all = SubSectionSpec("All", is_total_count=True)
        addr_no_storage = SubSectionSpec(
            "Without Server Storage", key="no_server_storage"
        )
        no_data = SubSectionSpec("No Data", key="no_server_storage_or_data")
        has_data = SubSectionSpec("Has Data", key="no_server_storage_but_data")
        cleaned = SubSectionSpec("Cleaned", is_clean_count=True)

        addr_all.subsections = [addr_no_storage]
        addr_no_storage.subsections = [no_data, has_data]
        has_data.subsections = [cleaned]

        return [
            SectionSpec("Profiles", subsections=[prof_all]),
            SectionSpec("Relay Addresses", subsections=[addr_all]),
            SectionSpec("Domain Addresses", subsections=[addr_all]),
        ]


class MissingProfileCleaner(CleanerTask):
    slug = "missing-profile"
    title = "Ensures users have a profile"
    check_description = "All users should have one profile."

    def _get_counts_and_data(self) -> tuple[Counts, CleanupData]:
        """
        Find users without profiles.

        Returns:
        * counts: two-level dict of summary and user counts
        * cleanup_data: empty dict
        """

        # Construct user -> profile counts
        users_with_profile_counts = User.objects.annotate(num_profiles=Count("profile"))
        ok_users = users_with_profile_counts.filter(num_profiles__gte=1)
        no_profile_users = users_with_profile_counts.filter(num_profiles=0)

        # Get counts once
        ok_user_count = ok_users.count()
        no_profile_user_count = no_profile_users.count()

        # Return counts and (empty) cleanup data
        counts: Counts = {
            "summary": {
                "ok": ok_user_count,
                "needs_cleaning": no_profile_user_count,
            },
            "users": {
                "all": User.objects.count(),
                "no_profile": no_profile_user_count,
                "has_profile": ok_user_count,
            },
        }
        cleanup_data: CleanupData = {"users": no_profile_users}
        return counts, cleanup_data

    def _clean(self) -> int:
        """Assign users to groups and create profiles."""
        counts = self.counts
        counts["users"]["cleaned"] = 0
        for user in self.cleanup_data["users"]:
            create_user_profile(sender=User, instance=user, created=True)
            counts["users"]["cleaned"] += 1
        return counts["users"]["cleaned"]

    def markdown_report_spec(self) -> list[SectionSpec]:
        """
        Return specification for MissingProfileCleaner markdown report

        - Profiles
          - All
            - Has Profile
            - No Profile
              - Now has Profile (if --clean)
        """
        user_all = SubSectionSpec("All", is_total_count=True)
        has_profile = SubSectionSpec("Has Profile")
        no_profile = SubSectionSpec("No Profile")
        cleaned = SubSectionSpec("Now has Profile", key="cleaned", is_clean_count=True)

        no_profile.subsections = [cleaned]
        user_all.subsections = [has_profile, no_profile]

        return [SectionSpec("Users", subsections=[user_all])]
