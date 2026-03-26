import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

def test_pad_cik():
    from ingestion.edgar import _pad_cik
    assert _pad_cik("0001336528") == "0001336528"
    assert _pad_cik("1336528") == "0001336528"
    assert len(_pad_cik("1234")) == 10


def test_parse_13f_xml():
    from ingestion.edgar import parse_13f_xml

    sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>1500000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>8500000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>8500000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>594918104</cusip>
    <value>800000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>2000000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>2000000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
</informationTable>"""

    holdings = parse_13f_xml(sample_xml)
    assert len(holdings) == 2

    aapl = next(h for h in holdings if h["cusip"] == "037833100")
    assert aapl["shares"] == 8500000
    assert aapl["value_usd"] == 1500000 * 1000  # value in thousands


def test_cusip_known_mapping():
    from ingestion.edgar import cusip_to_ticker_lookup
    ticker = cusip_to_ticker_lookup("037833100", "APPLE INC", {})
    assert ticker == "AAPL"

    ticker = cusip_to_ticker_lookup("594918104", "MICROSOFT CORP", {})
    assert ticker == "MSFT"


def test_get_filings_of_type():
    from ingestion.edgar import get_filings_of_type
    submissions = {
        "filings": {
            "recent": {
                "form": ["13F-HR", "4", "13F-HR", "SC 13D"],
                "filingDate": ["2024-11-14", "2024-10-01", "2024-08-14", "2024-07-01"],
                "accessionNumber": ["0001-24-001", "0001-24-002", "0001-24-003", "0001-24-004"],
            }
        }
    }

    filings = get_filings_of_type(submissions, "13F-HR")
    assert len(filings) == 2
    assert all(f["form_type"] == "13F-HR" for f in filings)

    filings_13d = get_filings_of_type(submissions, "SC 13D")
    assert len(filings_13d) == 1
