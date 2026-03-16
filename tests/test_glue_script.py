import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from glue.scripts import search_keyword_performance as glue_script


class TestGlueHelpers:
    def test_domain_google(self):
        assert glue_script._domain_from_referrer(
            "http://www.google.com/search?q=ipod"
        ) == "google.com"

    def test_keyword_yahoo(self):
        assert glue_script._keyword_from_referrer(
            "http://search.yahoo.com/search?p=cd+player"
        ) == "cd player"

    def test_external_search(self):
        assert glue_script.is_external_search(
            "http://www.bing.com/search?q=zune"
        ) is True

    def test_internal_referrer_not_external(self):
        assert glue_script.is_external_search(
            "http://www.esshopzilla.com/search/?k=Ipod"
        ) is False

    def test_purchase_event(self):
        assert glue_script.has_purchase("2,1,200") is True
        assert glue_script.has_purchase("2,200") is False

    def test_revenue_parsing(self):
        revenue = glue_script.revenue_from_product_list(
            "Electronics;Zune - 32GB;1;250;,Office Supplies;Red Folders;4;4.00;"
        )
        assert revenue == 254.0

    def test_output_filename(self):
        assert glue_script.output_filename(date(2009, 10, 8)) == (
            "2009-10-08_SearchKeywordPerformance.tab"
        )
