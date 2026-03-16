import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from local.processor import SearchKeywordProcessor, PURCHASE_EVENT, SEARCH_ENGINE_HOSTS


class TestReferrerParsing:
    def test_domain_google(self):
        assert SearchKeywordProcessor._domain_from_referrer(
            "http://www.google.com/search?q=ipod"
        ) == "google.com"

    def test_domain_yahoo(self):
        assert SearchKeywordProcessor._domain_from_referrer(
            "http://search.yahoo.com/search?p=cd+player"
        ) == "search.yahoo.com"

    def test_domain_bing(self):
        assert SearchKeywordProcessor._domain_from_referrer(
            "http://www.bing.com/search?q=Zune"
        ) == "bing.com"

    def test_domain_empty(self):
        assert SearchKeywordProcessor._domain_from_referrer("") is None
        assert SearchKeywordProcessor._domain_from_referrer(None) is None

    def test_keyword_q_param(self):
        assert SearchKeywordProcessor._keyword_from_referrer(
            "http://www.google.com/search?q=Ipod"
        ) == "Ipod"

    def test_keyword_p_param(self):
        assert SearchKeywordProcessor._keyword_from_referrer(
            "http://search.yahoo.com/search?p=cd+player"
        ) == "cd player"


class TestExternalSearch:
    def test_google_is_external(self):
        assert SearchKeywordProcessor.is_external_search_referrer(
            "http://www.google.com/search?q=ipod"
        ) is True

    def test_yahoo_is_external(self):
        assert SearchKeywordProcessor.is_external_search_referrer(
            "http://search.yahoo.com/search?p=cd+player"
        ) is True

    def test_bing_is_external(self):
        assert SearchKeywordProcessor.is_external_search_referrer(
            "http://www.bing.com/search?q=Zune"
        ) is True

    def test_internal_referrer_not_external(self):
        assert SearchKeywordProcessor.is_external_search_referrer(
            "http://www.esshopzilla.com/search/?k=Ipod"
        ) is False


class TestPurchaseEvent:
    def test_has_purchase(self):
        assert SearchKeywordProcessor.has_purchase_event("1") is True
        assert SearchKeywordProcessor.has_purchase_event("2,1,200") is True

    def test_no_purchase(self):
        assert SearchKeywordProcessor.has_purchase_event("2,200") is False
        assert SearchKeywordProcessor.has_purchase_event("") is False


class TestRevenueFromProductList:
    def test_single_product(self):
        rev = SearchKeywordProcessor.revenue_from_product_list(
            "Electronics;Zune - 32GB;1;250;"
        )
        assert rev == 250.0

    def test_multiple_products(self):
        rev = SearchKeywordProcessor.revenue_from_product_list(
            "Electronics;Zune - 32GB;1;250;,Office Supplies;Red Folders;4;4.00;"
        )
        assert rev == 254.0

    def test_empty(self):
        assert SearchKeywordProcessor.revenue_from_product_list("") == 0.0


class TestProcessRows:
    def test_aggregate_and_sort(self):
        processor = SearchKeywordProcessor()
        lines = [
            "hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer",
            "1\t2009-09-27 06:34:40\tMozilla\t1.2.3.4\t1\tA\tB\tUS\tHome\thttp://x.com\tElectronics;Zune;1;100;\thttp://www.google.com/search?q=zune",
            "2\t2009-09-27 07:00:00\tMozilla\t1.2.3.5\t1\tA\tB\tUS\tHome\thttp://x.com\tElectronics;Ipod;1;50;\thttp://www.google.com/search?q=zune",
        ]
        rows = processor.process_tsv_lines(lines)
        assert len(rows) == 1
        assert rows[0].search_engine_domain == "google.com"
        assert rows[0].search_keyword == "zune"
        assert rows[0].revenue == 150.0

    def test_output_filename(self):
        name = SearchKeywordProcessor.output_filename(date(2009, 10, 8))
        assert name == "2009-10-08_SearchKeywordPerformance.tab"
