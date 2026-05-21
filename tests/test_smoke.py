def test_db_fixture_has_listings_table(db):
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(listings)")}
    assert "latitude" in cols
    assert "cross_platform_group_id" in cols
