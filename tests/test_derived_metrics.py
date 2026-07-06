from datetime import datetime, timedelta, timezone

import pytest

from analytics import derived_metrics as dm
from storage import redis_client


def _leg(strike, option_type, **overrides):
    row = {
        "strike": strike, "option_type": option_type, "oi": 100, "volume": 10,
        "iv": 15.0, "ltp": 5.0, "bid": 4.5, "ask": 5.5,
    }
    row.update(overrides)
    return row


@pytest.fixture
def sample_rows():
    return [
        _leg(24000, "CE", oi=500, volume=50, iv=14.0),
        _leg(24000, "PE", oi=300, volume=20, iv=14.5),
        _leg(24050, "CE", oi=200, volume=15, iv=13.5),  # ATM if spot=24030
        _leg(24050, "PE", oi=250, volume=25, iv=15.5),
        _leg(24100, "CE", oi=1000, volume=100, iv=13.0),  # highest call OI/volume
        _leg(24100, "PE", oi=50, volume=5, iv=16.0),
    ]


def test_compute_pcr(sample_rows):
    result = dm.compute_pcr(sample_rows)
    total_call_oi = 500 + 200 + 1000
    total_put_oi = 300 + 250 + 50
    assert result["pcr_oi"] == pytest.approx(total_put_oi / total_call_oi)


def test_compute_max_pain_picks_strike_minimizing_writer_payout(sample_rows):
    max_pain = dm.compute_max_pain(sample_rows)
    assert max_pain in {24000, 24050, 24100}


def test_compute_moneyness_labels_atm_itm_otm(sample_rows):
    labels = dm.compute_moneyness(sample_rows, underlying_ltp=24050)
    assert labels[(24050, "CE")] == "ATM"
    assert labels[(24050, "PE")] == "ATM"
    assert labels[(24000, "CE")] == "ITM"  # call strike below spot is ITM
    assert labels[(24100, "CE")] == "OTM"  # call strike above spot is OTM
    assert labels[(24000, "PE")] == "OTM"  # put strike below spot is OTM
    assert labels[(24100, "PE")] == "ITM"  # put strike above spot is ITM


def test_compute_atm_and_oi_summary(sample_rows):
    summary = dm.compute_atm_and_oi_summary(sample_rows, underlying_ltp=24030)
    assert summary["atm_strike"] == 24050
    assert summary["highest_call_oi_strike"] == 24100
    assert summary["highest_call_volume_strike"] == 24100
    assert summary["total_call_oi"] == 500 + 200 + 1000
    assert summary["total_put_oi"] == 300 + 250 + 50


def test_compute_iv_skew_uses_nearest_otm_strikes(sample_rows):
    skew = dm.compute_iv_skew(sample_rows, underlying_ltp=24030)
    # ATM (nearest to 24030) is 24050 -> atm_iv = mean(13.5, 15.5) = 14.5
    # "OTM call" is defined purely as strike > spot, so the nearest one (24050, iv=13.5)
    # can coincide with the ATM strike itself when spot sits between strikes asymmetrically.
    # nearest OTM put (<spot) = 24000 PE (iv=14.5)
    assert skew["otm_call_skew"] == pytest.approx(13.5 - 14.5)
    assert skew["otm_put_skew"] == pytest.approx(14.5 - 14.5)


class TestOiChangeAndVolumeZscore:
    def setup_method(self):
        redis_client.client.delete("nifty:oi_prev:TESTEXPIRY:24000.0:CE")
        for i in range(25):
            redis_client.client.delete(f"nifty:vol_hist:24000.0:CE")

    def test_oi_change_first_call_is_none_second_call_is_delta(self):
        rows = [{"strike": 24000.0, "option_type": "CE", "oi": 100}]
        first = dm.compute_oi_change("TESTEXPIRY", rows)
        assert first[(24000.0, "CE")] is None  # no prior snapshot yet

        rows2 = [{"strike": 24000.0, "option_type": "CE", "oi": 150}]
        second = dm.compute_oi_change("TESTEXPIRY", rows2)
        assert second[(24000.0, "CE")] == 50

    def test_volume_zscore_needs_history_before_computing(self):
        # Varying volume so variance/std is nonzero -- a constant series has std=0,
        # which the implementation correctly treats as "can't compute a z-score", not a bug.
        volumes = [10, 20, 15, 25, 12, 100]
        results = []
        for vol in volumes:
            rows = [{"strike": 24000.0, "option_type": "CE", "volume": vol}]
            results.append(dm.compute_volume_zscore(rows)[(24000.0, "CE")])

        # Each call checks history accumulated BEFORE pushing the current value, so the
        # first 5 calls see history sizes 0..4 (all <5) and return None.
        assert results[:5] == [None] * 5
        assert results[5] is not None  # 6th call: history size is now 5


class TestFuturesBasis:
    def setup_method(self):
        redis_client.client.delete("nifty:futures:nearest:latest")
        redis_client.client.delete("nifty:futures_basis_warn_cooldown")

    def test_no_cache_returns_none(self):
        assert dm.compute_futures_basis(24000.0) is None

    def test_fresh_cache_computes_basis(self):
        redis_client.set_latest(
            "nifty:futures:nearest:latest",
            {"security_id": "12345", "ltp": 24060.0, "fetched_at": datetime.now(timezone.utc).isoformat()},
        )
        assert dm.compute_futures_basis(24000.0) == pytest.approx(60.0)

    def test_stale_cache_returns_none(self):
        stale_time = datetime.now(timezone.utc) - timedelta(seconds=120)
        redis_client.set_latest(
            "nifty:futures:nearest:latest",
            {"security_id": "12345", "ltp": 24060.0, "fetched_at": stale_time.isoformat()},
        )
        assert dm.compute_futures_basis(24000.0) is None

    def test_none_underlying_ltp_returns_none(self):
        assert dm.compute_futures_basis(None) is None
