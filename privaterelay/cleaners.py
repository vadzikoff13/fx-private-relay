"""Framework for tasks that identify data issues and (if possible) clean them up"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

Counts = dict[str, dict[str, int]]
CleanupData = dict[str, Any]


@dataclass
class SectionSpec:
    """
    Specify a top-level section of a markdown report.

    - name: The display name of the section
    - key: The key into the counts dict, if it can not be guessed from the name
    - subsections: A list of SubSectionSpec below this section
    """

    name: str
    key: Optional[str] = None
    subsections: list["SubSectionSpec"] = field(default_factory=list)

    def get_key(self) -> str:
        """Return key to value in counts dict."""
        if self.key:
            return self.key
        else:
            return self.name.lower().replace(" ", "_")


@dataclass
class SubSectionSpec(SectionSpec):
    """
    Specify a lower-level section of a markdown report.

    It has the fields in SectionSpec and these additional fields:

    - is_total_count: This is a top-level total, like the count of all users.
      The section is always displayed, even if zero. It is an error if
      the key is not in the counts dict.
    - is_clean_count: This is a count of a cleaning action. If --clean was not
      specified, the key will not be in counts dict, and it should not be
      displayed. If --clean is specified, the key will be in the counts dict,
      and should be displayed even if zero.

    If neither is set, then if all sections at the same level are missing or
    zero, the sections are not displayed and lower sections are not processed.
    """

    is_total_count: bool = False
    is_clean_count: bool = False


class DataIssueTask:
    """Base class for data issue / cleaner tasks."""

    slug: str  # Short name, appropriate for command-line option
    title: str  # Short title for reports
    check_description: str  # A sentence describing what this cleaner is checking.
    can_clean: bool  # True if the issue can be automatically cleaned

    _counts: Optional[Counts]
    _cleanup_data: Optional[CleanupData]
    _cleaned: bool

    def __init__(self) -> None:
        self._counts = None
        self._cleanup_data = None
        self._cleaned = False

    @property
    def counts(self) -> Counts:
        """Get relevant counts for data issues and prepare to clean if possible."""
        if self._counts is None:
            assert self._cleanup_data is None
            self._counts, self._cleanup_data = self._get_counts_and_data()
        return self._counts

    @property
    def cleanup_data(self) -> CleanupData:
        """Get data needed to clean data issues."""
        assert self.counts  # Populate self._cleanup_data if not populated
        assert self._cleanup_data
        return self._cleanup_data

    def issues(self) -> int:
        """Return the number of detected data issues."""
        return self.counts["summary"]["needs_cleaning"]

    def _get_counts_and_data(self) -> tuple[Counts, CleanupData]:
        """Return a dictionary of counts and cleanup data."""
        raise NotImplementedError("_get_counts_and_data() not implemented")

    def _clean(self) -> int:
        """
        Clean the detected items.

        Returns the number of cleaned items. Implementors can add detailed
        counts to self._counts as needed.
        """
        raise NotImplementedError("_clean() not implemented")

    def clean(self) -> int:
        """Clean the detected items, and update counts["summary"]"""
        summary = self.counts["summary"]
        if not self._cleaned:
            summary["cleaned"] = self._clean()
            self._cleaned = True
        return summary["cleaned"]

    def markdown_report_spec(self) -> list[SectionSpec]:
        """Return specification for the markdown report"""
        raise NotImplementedError("markdown_report_spec() not implemented")

    def _markdown_subsections(
        self,
        subsections: list[SubSectionSpec],
        counts: dict[str, int],
        level: int = 1,
        parent_count: Optional[int] = None,
    ) -> list[str]:
        """
        Recursively generate the markdown for a subsection.

        - subsections - the list of subsection specs to generate
        - counts - the collection of counts for this and related sections
        - level - the recursion level, used for indenting
        - parent_count - the parent section's count, used for percentages.

        Return is a list of strings at this level and below, one per line
        """
        assert subsections

        subcounts: dict[str, int] = {}
        percents: dict[str, str] = {}
        any_found = False
        display = False

        # Gather counts at this level
        for section in subsections:
            key = section.get_key()
            try:
                count = counts[key] or 0
            except KeyError:
                if section.is_total_count:
                    raise
                count = 0
            else:
                any_found = True
                display |= section.is_total_count or section.is_clean_count

            subcounts[section.name] = count
            if parent_count:
                percents[section.name] = f"{count / parent_count:0.1%}"

        # Return early if non-required section is all zeros
        if not (any_found or display) and all(cnt == 0 for cnt in subcounts.values()):
            return []

        # Determine widths of names, counts, and percents
        max_name = max(len(key) for key in subcounts.keys())
        max_count = max(len(str(cnt)) for cnt in subcounts.values())
        max_percent = max(len(pct) for pct in percents.values()) if percents else 0

        # Construct sections
        lines: list[str] = []
        indent = level * 2
        for section in subsections:
            name = section.name
            count = subcounts[name]
            if parent_count:
                lines.append(
                    f"{' ' * indent}{section.name:<{max_name}}:{count: {max_count}d}"
                    f" ({percents[name]:>{max_percent}})"
                )
            else:
                lines.append(
                    f"{' ' * indent}{section.name:<{max_name}}:{count: {max_count}d}"
                )
            if count and section.subsections:
                lines.extend(
                    self._markdown_subsections(
                        subsections=section.subsections,
                        counts=counts,
                        level=level + 1,
                        parent_count=count,
                    )
                )
        return lines

    def markdown_report(self) -> str:
        """Generate the markdown report from the specification."""
        spec = self.markdown_report_spec()
        lines: list[str] = []

        for section in spec:
            key = section.get_key()
            counts = self.counts[key]
            lines.append(f"{section.name}:")
            lines.extend(
                self._markdown_subsections(
                    subsections=section.subsections, counts=counts
                )
            )

        return "\n".join(lines)


class CleanerTask(DataIssueTask):
    """Base class for tasks that can clean up detected issues."""

    can_clean = True


class DetectorTask(DataIssueTask):
    """Base class for tasks that cannot clean up detected issues."""

    can_clean = False

    def _clean(self) -> int:
        """DetectorTask can't clean any detected issues."""
        return 0
