"""
Attribute revenue to external search (Google, Yahoo, Bing, etc.) and keyword.
Only counts revenue when event_list has purchase (1). Output: domain, keyword, revenue.
"""
from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from urllib.parse import parse_qs, urlparse

PURCHASE_EVENT = "1"
SEARCH_ENGINE_HOSTS = {
    "google.com", "www.google.com",
    "search.yahoo.com", "yahoo.com", "www.yahoo.com",
    "bing.com", "www.bing.com", "msn.com", "search.msn.com",
}
KEYWORD_PARAMS = ("q", "p")


@dataclass
class SearchKeywordRow:
    search_engine_domain: str
    search_keyword: str
    revenue: float

    def to_tsv_row(self) -> list[str]:
        return [self.search_engine_domain, self.search_keyword, f"{self.revenue:.2f}"]


class SearchKeywordProcessor:
    """
    Read hit-level TSV, keep rows that are (external search referrer + purchase),
    sum revenue from product_list by (domain, keyword). Output sorted by revenue desc.
    """

    def __init__(self) -> None:
        self._agg: dict[tuple[str, str], float] = defaultdict(float)

    @staticmethod
    def _has_text(value: str | None) -> bool:
        return bool(value and value.strip())

    @staticmethod
    def _domain_from_referrer(referrer: str | None) -> str | None:
        if not SearchKeywordProcessor._has_text(referrer):
            return None
        try:
            parsed = urlparse(referrer.strip())
            host = (parsed.netloc or "").lower()
            if host.startswith("www."):
                host = host[4:]
            return host or None
        except Exception:
            return None

    @staticmethod
    def _keyword_from_referrer(referrer: str | None) -> str:
        if not SearchKeywordProcessor._has_text(referrer):
            return ""
        try:
            parsed = urlparse(referrer.strip())
            qs = parse_qs(parsed.query)
            for param in KEYWORD_PARAMS:
                if param in qs and qs[param]:
                    return (qs[param][0] or "").strip()
            return ""
        except Exception:
            return ""

    @staticmethod
    def is_external_search_referrer(referrer: str | None) -> bool:
        domain = SearchKeywordProcessor._domain_from_referrer(referrer)
        if not domain:
            return False
        return domain in SEARCH_ENGINE_HOSTS or f"www.{domain}" in SEARCH_ENGINE_HOSTS

    @staticmethod
    def has_purchase_event(event_list: str | None) -> bool:
        if not SearchKeywordProcessor._has_text(event_list):
            return False
        parts = [part.strip() for part in event_list.split(",") if part.strip()]
        return PURCHASE_EVENT in parts

    @staticmethod
    def revenue_from_product_list(product_list: str | None) -> float:
        if not SearchKeywordProcessor._has_text(product_list):
            return 0.0
        total = 0.0
        for product in product_list.split(","):
            product = product.strip()
            if not product:
                continue
            parts = product.split(";")
            if len(parts) >= 4 and parts[3]:
                try:
                    total += float(parts[3].strip())
                except ValueError:
                    pass
        return total

    def process_row(self, row: list[str], header: list[str]) -> None:
        if len(row) < len(header):
            return

        by_name = dict(zip(header, row))
        referrer = by_name.get("referrer", "")
        event_list = by_name.get("event_list", "")
        product_list = by_name.get("product_list", "")

        if not self.is_external_search_referrer(referrer):
            return
        if not self.has_purchase_event(event_list):
            return

        revenue = self.revenue_from_product_list(product_list)
        if revenue <= 0:
            return

        domain = self._domain_from_referrer(referrer) or "unknown"
        keyword = self._keyword_from_referrer(referrer) or "(not set)"
        self._agg[(domain, keyword)] += revenue

    def process_tsv_file(self, path: str) -> list[SearchKeywordRow]:
        self._agg.clear()
        with open(path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter="\t")
            header = next(reader, None)
            if not header:
                return []
            header = [column.strip() for column in header]
            for row in reader:
                if row:
                    self.process_row(row, header)

        rows = [SearchKeywordRow(domain, keyword, revenue) for (domain, keyword), revenue in self._agg.items()]
        rows.sort(key=lambda row: row.revenue, reverse=True)
        return rows

    def process_tsv_lines(self, lines: list[str]) -> list[SearchKeywordRow]:
        self._agg.clear()
        if not lines:
            return []

        reader = csv.reader(lines, delimiter="\t")
        header = [column.strip() for column in next(reader)]
        for row in reader:
            if row:
                self.process_row(row, header)

        rows = [SearchKeywordRow(domain, keyword, revenue) for (domain, keyword), revenue in self._agg.items()]
        rows.sort(key=lambda row: row.revenue, reverse=True)
        return rows

    @staticmethod
    def output_filename(execution_date: date | None = None) -> str:
        current_date = execution_date or date.today()
        return f"{current_date.isoformat()}_SearchKeywordPerformance.tab"

    @staticmethod
    def write_output(rows: list[SearchKeywordRow], path: str) -> None:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t")
            writer.writerow(["Search Engine Domain", "Search Keyword", "Revenue"])
            for row in rows:
                writer.writerow(row.to_tsv_row())
