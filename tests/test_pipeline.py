from pathlib import Path
from src.main import run_pipeline


def test_full_pipeline_execution(tmp_path: Path):
    test_db = (tmp_path / "test_pipeline.db").as_posix()
    db_url = f"sqlite:///{test_db}"

    res = run_pipeline(
        mode="replace",
        database_url=db_url,
        simulate_stream=True,
        force_ingest=False,
    )

    assert res["dq_status"] == "PASSED"
    assert res["dq_passed_checks"] == 8
    assert res["facts"]["fact_pos_sales"] > 0
    assert res["duration_sec"] > 0
