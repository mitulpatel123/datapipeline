import logging

import scripts.migrate_phase1_schema as migrate


def test_run_twice_in_a_row_does_not_crash():
    """The real acceptance test from the fix spec: run it once, run it again,
    confirm the second run doesn't crash and reports "already exists" instead of
    re-running DDL. Runs against the real local Postgres (already required
    infrastructure for this whole test suite) since a migration script's entire
    job is to react correctly to the database's actual current state."""
    migrate.run()  # first run: may create or may already find everything present
    migrate.run()  # second run: must be a no-op, not an error


def test_second_run_reports_already_exists(caplog):
    migrate.run()  # ensure everything is created first
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="migrate_phase1_schema"):
        migrate.run()

    messages = [record.message for record in caplog.records]
    assert any("Already exists" in m for m in messages), (
        f"expected at least one 'Already exists' message on a repeat run, got: {messages}"
    )
    assert not any("FAILED" in m for m in messages)


def test_column_exists_helper_reflects_real_schema():
    from sqlalchemy import inspect

    from storage.postgres_client import engine

    inspector = inspect(engine)
    # security_id was added by this exact migration script -- if this is False,
    # either the migration never ran or the column-existence check itself is broken.
    assert migrate._column_exists(inspector, "option_chain_snapshots", "security_id") is True
    assert migrate._column_exists(inspector, "option_chain_snapshots", "definitely_not_a_real_column") is False
