# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""State snapshot API tests: save, load, list, version, delete.

Covers the full state versioning API surface with the channel_states table.

Requires Actae server with versioned state support (migration v5).
"""

import json

import pytest



def _delete(actae, path):
    return actae._http.delete(
        f"{actae._cfg.api_url}{path}",
        headers=actae._cfg.api_headers,
    )


class TestSaveState:
    def test_save_state_returns_version(self, actae, ch_name):
        body = {
            "channel_id": ch_name,
            "cursor": 1,
            "state": {"key": "value"},
        }
        resp = actae.raw_post(f"/api/v1/state/{ch_name}", body)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert "version" in data
        assert isinstance(data["version"], int)
        assert data["version"] >= 1

    def test_save_state_versions_increment(self, actae, ch_name):
        v1 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": {"n": 1}},
        ).json()["version"]
        v2 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 2, "state": {"n": 2}},
        ).json()["version"]
        v3 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 3, "state": {"n": 3}},
        ).json()["version"]
        assert v2 > v1
        assert v3 > v2

    def test_save_state_preserves_complex_payload(self, actae, ch_name):
        state = {
            "context": {
                "messages": [
                    {"role": "user", "content": "Hello"},
                    {"role": "assistant", "content": "Hi there"},
                ],
                "tool_calls": [
                    {"tool": "search", "args": {"query": "weather"}, "result": "sunny"},
                ],
            },
            "completed_steps": [1, 2, 3],
            "metadata": {"agent": "test", "version": "1.0"},
        }
        body = {"channel_id": ch_name, "cursor": 42, "state": state}
        resp = actae.raw_post(f"/api/v1/state/{ch_name}", body)
        assert resp.status_code == 200

        load = actae.raw_get(f"/api/v1/state/{ch_name}")
        assert load.status_code == 200
        loaded = load.json()
        assert loaded["cursor"] == 42
        assert loaded["state"]["completed_steps"] == [1, 2, 3]
        assert loaded["state"]["context"]["messages"][1]["content"] == "Hi there"

    def test_save_state_requires_channel_id(self, actae, ch_name):
        body = {"channel_id": "", "cursor": 1, "state": {}}
        resp = actae.raw_post(f"/api/v1/state/{ch_name}", body)
        assert resp.status_code == 400

    def test_save_state_all_jsonb_types_preserved(self, actae, ch_name):
        state = {
            "null_val": None,
            "bool_true": True,
            "bool_false": False,
            "int_zero": 0,
            "int_positive": 42,
            "int_negative": -17,
            "float_positive": 3.14,
            "float_negative": -2.718,
            "empty_string": "",
            "simple_string": "hello world",
            "unicode": "héllo ",
            "escaped": "line1\nline2\t\"quoted\"\\backslash",
            "empty_array": [],
            "empty_object": {},
            "mixed_array": [None, True, 42, "str", {"nested": False}],
        }
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": state},
        )
        loaded = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        s = loaded["state"]
        assert s["null_val"] is None
        assert s["bool_true"] is True
        assert s["bool_false"] is False
        assert s["int_zero"] == 0
        assert s["int_negative"] == -17
        assert s["float_positive"] == 3.14
        assert s["empty_string"] == ""
        assert s["unicode"] == "héllo "
        assert s["escaped"] == "line1\nline2\t\"quoted\"\\backslash"
        assert s["empty_array"] == []
        assert s["empty_object"] == {}
        assert s["mixed_array"] == [None, True, 42, "str", {"nested": False}]

    def test_save_state_empty_object(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": {}},
        )
        loaded = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        assert loaded["state"] == {}

    def test_save_state_cursor_zero(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 0, "state": {"at": "zero"}},
        )
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == 0

    def test_save_state_cursor_negative(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": -5, "state": {"behind": True}},
        )
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == -5

    def test_save_state_deeply_nested_jsonb(self, actae, ch_name):
        def build_nested(depth):
            if depth == 0:
                return {"leaf": True, "value": depth}
            return {"child": build_nested(depth - 1), "depth": depth}
        state = build_nested(20)
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 42, "state": state},
        )
        loaded = actae.raw_get(f"/api/v1/state/{ch_name}").json()["state"]
        cursor = loaded
        for expected_depth in range(20, 0, -1):
            assert cursor["depth"] == expected_depth
            cursor = cursor["child"]
        assert cursor["leaf"] is True

    def test_save_state_large_payload(self, actae, ch_name):
        items = [{"id": i, "content": f"item-{i}" * 20} for i in range(200)]
        state = {"items": items, "metadata": {"count": len(items)}}
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": state},
        )
        loaded = actae.raw_get(f"/api/v1/state/{ch_name}").json()["state"]
        assert len(loaded["items"]) == 200
        assert loaded["items"][199]["id"] == 199


class TestLoadLatestState:
    def test_load_latest_returns_cursor_and_state(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 100, "state": {"loaded": True}},
        )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cursor"] == 100
        assert data["state"]["loaded"] is True

    def test_load_latest_returns_last_saved(self, actae, ch_name):
        for i in range(5):
            actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i * 10, "state": {"seq": i}},
            )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cursor"] == 40
        assert data["state"]["seq"] == 4

    def test_load_latest_returns_null_for_unknown_channel(self, actae, ch_name):
        resp = actae.raw_get(f"/api/v1/state/{ch_name}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cursor"] is None
        assert data["state"] is None

    def test_load_latest_after_delete_returns_next_latest(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 10, "state": {"v": 1}},
        )
        v2_resp = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 20, "state": {"v": 2}},
        )
        v3_resp = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 30, "state": {"v": 3}},
        )

        _delete(actae, f"/api/v1/state/{ch_name}/version/{v3_resp.json()['version']}")
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == 20

        _delete(actae, f"/api/v1/state/{ch_name}/version/{v2_resp.json()['version']}")
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == 10


class TestListStates:
    def test_list_states_returns_version_history(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 10, "state": {"n": 1}},
        )
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 20, "state": {"n": 2}},
        )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "100"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["channel_id"] == ch_name
        assert data["count"] == 2
        assert len(data["versions"]) == 2
        for v in data["versions"]:
            assert "version" in v
            assert "cursor" in v
            assert "timestamp" in v
            assert isinstance(v["version"], int)
            assert isinstance(v["cursor"], int)
            assert isinstance(v["timestamp"], str)

    def test_list_states_ordered_by_version_desc(self, actae, ch_name):
        for i in range(5):
            actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i, "state": {"n": i}},
            )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "100"})
        versions = resp.json()["versions"]
        for i in range(len(versions) - 1):
            assert versions[i]["version"] > versions[i + 1]["version"], \
                f"version {versions[i]['version']} should be > {versions[i+1]['version']}"

    def test_list_states_respects_limit(self, actae, ch_name):
        for i in range(10):
            actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i, "state": {"n": i}},
            )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "3"})
        assert len(resp.json()["versions"]) == 3
        assert resp.json()["count"] == 3

    def test_list_states_respects_offset(self, actae, ch_name):
        for i in range(10):
            actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i, "state": {"n": i}},
            )
        page1 = actae.raw_get(
            f"/api/v1/state/{ch_name}/versions", params={"limit": "3", "offset": "0"}
        ).json()
        page2 = actae.raw_get(
            f"/api/v1/state/{ch_name}/versions", params={"limit": "3", "offset": "3"}
        ).json()
        p1_versions = [v["version"] for v in page1["versions"]]
        p2_versions = [v["version"] for v in page2["versions"]]
        assert len(set(p1_versions) & set(p2_versions)) == 0, "pages should not overlap"

    def test_list_states_empty_channel(self, actae, ch_name):
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "100"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["versions"] == []

    def test_list_states_limit_one(self, actae, ch_name):
        for i in range(5):
            actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i, "state": {"n": i}},
            )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "1"})
        assert len(resp.json()["versions"]) == 1
        assert resp.json()["versions"][0]["cursor"] == 4  # latest

    def test_list_states_offset_beyond_count(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": {"x": 1}},
        )
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "10", "offset": "100"})
        assert len(resp.json()["versions"]) == 0
        assert resp.json()["count"] == 0


class TestLoadStateVersion:
    def test_load_specific_version(self, actae, ch_name):
        v1_response = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 100, "state": {"v": 1}},
        )
        v1 = v1_response.json()["version"]
        v2_response = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 200, "state": {"v": 2}},
        )
        v2 = v2_response.json()["version"]

        resp1 = actae.raw_get(f"/api/v1/state/{ch_name}/version/{v1}")
        assert resp1.status_code == 200
        assert resp1.json()["cursor"] == 100
        assert resp1.json()["state"]["v"] == 1
        assert resp1.json()["version"] == v1

        resp2 = actae.raw_get(f"/api/v1/state/{ch_name}/version/{v2}")
        assert resp2.status_code == 200
        assert resp2.json()["cursor"] == 200
        assert resp2.json()["state"]["v"] == 2
        assert resp2.json()["version"] == v2

    def test_load_version_not_found(self, actae, ch_name):
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/99999")
        assert resp.status_code == 404
        assert "version not found" in resp.json()["error"]

    def test_load_version_from_empty_channel(self, actae, ch_name):
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/1")
        assert resp.status_code == 404

    def test_load_version_negative_version(self, actae, ch_name):
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/-1")
        assert resp.status_code == 404

    def test_load_version_zero(self, actae, ch_name):
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/0")
        assert resp.status_code == 404


class TestDeleteStateVersion:
    def test_delete_removes_only_target_version(self, actae, ch_name):
        v1 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 10, "state": {"keep": True}},
        ).json()["version"]
        v2 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 20, "state": {"keep": False}},
        ).json()["version"]
        v3 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 30, "state": {"keep": True}},
        ).json()["version"]

        del_resp = _delete(actae, f"/api/v1/state/{ch_name}/version/{v2}")
        assert del_resp.status_code == 200
        assert del_resp.json()["status"] == "deleted"

        assert actae.raw_get(f"/api/v1/state/{ch_name}/version/{v2}").status_code == 404

        remaining = actae.raw_get(
            f"/api/v1/state/{ch_name}/versions", params={"limit": "100"}
        ).json()
        remaining_versions = [v["version"] for v in remaining["versions"]]
        assert len(remaining_versions) == 2
        assert v1 in remaining_versions
        assert v2 not in remaining_versions
        assert v3 in remaining_versions

        latest = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        assert latest["cursor"] == 30

    def test_delete_nonexistent_version_returns_ok(self, actae, ch_name):
        del_resp = _delete(actae, f"/api/v1/state/{ch_name}/version/12345")
        assert del_resp.status_code == 200
        assert del_resp.json()["status"] == "deleted"

    def test_delete_last_version_makes_channel_empty(self, actae, ch_name):
        v = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": {"x": 1}},
        ).json()["version"]
        _delete(actae, f"/api/v1/state/{ch_name}/version/{v}")
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] is None
        assert actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "100"}).json()["count"] == 0

    def test_double_delete_returns_ok(self, actae, ch_name):
        v = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": {"x": 1}},
        ).json()["version"]
        assert _delete(actae, f"/api/v1/state/{ch_name}/version/{v}").status_code == 200
        assert _delete(actae, f"/api/v1/state/{ch_name}/version/{v}").status_code == 200


class TestStateEndToEnd:
    def test_full_crash_resume_workflow(self, actae, ch_name):
        actae.record(ch_name, "agent.started", {"step": 1}, "agent")
        actae.record(ch_name, "agent.progress", {"step": 2}, "agent")
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {
                "channel_id": ch_name,
                "cursor": 2,
                "state": {
                    "completed_steps": [1, 2],
                    "current_step": 3,
                    "context": {"mood": "optimistic"},
                },
            },
        )

        loaded = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        assert loaded["cursor"] == 2
        assert loaded["state"]["completed_steps"] == [1, 2]
        assert loaded["state"]["current_step"] == 3

        actae.record(ch_name, "agent.progress", {"step": 3}, "agent")
        actae.record(ch_name, "agent.progress", {"step": 4}, "agent")
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {
                "channel_id": ch_name,
                "cursor": 4,
                "state": {"completed_steps": [1, 2, 3, 4], "current_step": 5},
            },
        )
        actae.record(ch_name, "agent.completed", {"step": 5}, "agent")

        latest = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        assert latest["cursor"] == 4
        assert latest["state"]["completed_steps"] == [1, 2, 3, 4]

        versions = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "100"}).json()
        assert versions["count"] == 2

        v1 = versions["versions"][1]["version"]
        first = actae.raw_get(f"/api/v1/state/{ch_name}/version/{v1}").json()
        assert first["state"]["completed_steps"] == [1, 2]
        assert first["state"]["context"]["mood"] == "optimistic"

    def test_versioning_across_multiple_channels(self, actae, ch_name):
        ch_a = f"{ch_name}-a"
        ch_b = f"{ch_name}-b"

        actae.raw_post(
            f"/api/v1/state/{ch_a}",
            {"channel_id": ch_a, "cursor": 1, "state": {"chan": "a"}},
        )
        actae.raw_post(
            f"/api/v1/state/{ch_b}",
            {"channel_id": ch_b, "cursor": 1, "state": {"chan": "b"}},
        )
        actae.raw_post(
            f"/api/v1/state/{ch_a}",
            {"channel_id": ch_a, "cursor": 2, "state": {"chan": "a-2"}},
        )

        a_vers = actae.raw_get(
            f"/api/v1/state/{ch_a}/versions", params={"limit": "100"}
        ).json()
        b_vers = actae.raw_get(
            f"/api/v1/state/{ch_b}/versions", params={"limit": "100"}
        ).json()
        assert a_vers["count"] == 2
        assert b_vers["count"] == 1

        a_latest = actae.raw_get(f"/api/v1/state/{ch_a}").json()
        assert a_latest["state"]["chan"] == "a-2"

        b_latest = actae.raw_get(f"/api/v1/state/{ch_b}").json()
        assert b_latest["state"]["chan"] == "b"

    def test_delete_then_reload_oldest_becomes_latest(self, actae, ch_name):
        v1 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 10, "state": {"v": 1}},
        ).json()["version"]
        v2 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 20, "state": {"v": 2}},
        ).json()["version"]
        v3 = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 30, "state": {"v": 3}},
        ).json()["version"]

        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == 30

        _delete(actae, f"/api/v1/state/{ch_name}/version/{v3}")
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == 20

        _delete(actae, f"/api/v1/state/{ch_name}/version/{v2}")
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] == 10

        _delete(actae, f"/api/v1/state/{ch_name}/version/{v1}")
        assert actae.raw_get(f"/api/v1/state/{ch_name}").json()["cursor"] is None

    def test_auth_required_for_state_endpoints(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        ch = "auth-test-channel"
        resp_bad = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/state/{ch}",
            json={"channel_id": ch, "cursor": 1, "state": {}},
        )
        assert resp_bad.status_code in (401, 400), f"expected auth error, got {resp_bad.status_code}"

    def test_rapid_fire_200_saves_then_verify(self, actae, ch_name):
        for i in range(200):
            actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i, "state": {"seq": i}},
            )
        versions = actae.raw_get(
            f"/api/v1/state/{ch_name}/versions", params={"limit": "250"}
        ).json()
        assert versions["count"] == 200
        assert versions["versions"][0]["cursor"] == 199
        assert versions["versions"][199]["cursor"] == 0

    def test_save_after_delete_all_increments(self, actae, ch_name):
        versions = []
        for i in range(3):
            v = actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i, "state": {"phase": i}},
            ).json()["version"]
            versions.append(v)

        for v in versions:
            _delete(actae, f"/api/v1/state/{ch_name}/version/{v}")

        new_v = actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 100, "state": {"phase": "reborn"}},
        ).json()["version"]
        assert new_v > max(versions)

    def test_channels_are_fully_independent(self, actae, ch_name):
        for ch_idx in range(5):
            ch = f"{ch_name}-{ch_idx}"
            for cursor in range(5):
                actae.raw_post(
                    f"/api/v1/state/{ch}",
                    {"channel_id": ch, "cursor": cursor, "state": {"ch": ch_idx}},
                )

        for ch_idx in range(5):
            ch = f"{ch_name}-{ch_idx}"
            vers = actae.raw_get(f"/api/v1/state/{ch}/versions", params={"limit": "100"}).json()
            assert vers["count"] == 5
            assert actae.raw_get(f"/api/v1/state/{ch}").json()["state"]["ch"] == ch_idx

    def test_load_specific_version_after_interleaved_operations(self, actae, ch_name):
        saved = {}
        for i in range(20):
            resp = actae.raw_post(
                f"/api/v1/state/{ch_name}",
                {"channel_id": ch_name, "cursor": i * 10, "state": {"id": i}},
            )
            saved[i] = resp.json()["version"]

            if i % 5 == 4:
                mid = actae.raw_get(f"/api/v1/state/{ch_name}").json()
                assert mid["cursor"] == i * 10

            if i % 3 == 0 and i > 0:
                _delete(actae, f"/api/v1/state/{ch_name}/version/{saved[i - 1]}")

        for i, v in saved.items():
            if i % 3 == 2:
                # These versions were explicitly deleted above (saved[i-1]
                # at i = 3, 6, 9, ...). Verify the delete took effect.
                resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/{v}")
                assert resp.status_code == 404, f"version {v} (i={i}) should be deleted"
                continue
            if i % 3 == 0 or i == 0:
                continue
            resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/{v}")
            assert resp.status_code == 200, f"version {v} (i={i}) should exist"
            assert resp.json()["state"]["id"] == i

    def test_timestamp_in_list_response(self, actae, ch_name):
        actae.raw_post(
            f"/api/v1/state/{ch_name}",
            {"channel_id": ch_name, "cursor": 1, "state": {"n": 1}},
        )
        versions = actae.raw_get(f"/api/v1/state/{ch_name}/versions", params={"limit": "1"}).json()
        ts = versions["versions"][0]["timestamp"]
        assert "T" in ts or "Z" in ts, f"timestamp '{ts}' should be ISO 8601"
        assert len(ts) > 10

