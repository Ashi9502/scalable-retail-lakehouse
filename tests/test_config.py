from src.common.config import load_config


def test_quality_threshold_is_sane():
    cfg = load_config()
    value = float(cfg["quality"]["max_rejected_ratio"])
    assert 0 <= value < 1


def test_required_status_exists():
    cfg = load_config()
    assert "DELIVERED" in cfg["quality"]["allowed_statuses"]
