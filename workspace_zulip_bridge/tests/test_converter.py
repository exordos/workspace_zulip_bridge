import copy
import uuid

import pytest

from workspace_zulip_bridge import canonical, converter, markdown_conversion

ACCOUNT_UUID = str(uuid.uuid4())
OWNER_UUID = str(uuid.uuid4())
PROJECT_UUID = str(uuid.uuid4())


class FakeStore:
    def __init__(
        self,
        selection_mode="all",
        auto_materialize=True,
        *,
        account_uuid=ACCOUNT_UUID,
        owner_uuid=OWNER_UUID,
        project_uuid=PROJECT_UUID,
    ):
        self.account_uuid = account_uuid
        self.owner_uuid = owner_uuid
        self.project_uuid = project_uuid
        self.account = {
            "generation": 1,
            "owner_user_uuid": owner_uuid,
            "settings": {
                "selection_mode": selection_mode,
                "default_project_id": project_uuid,
            },
        }
        self.assignments = {}
        self.mappings = {}
        self.positions = {}
        self.accepted_contexts = {}
        self.pending_contexts = {}
        self.auto_materialize = auto_materialize

    def account_resource(self, account_uuid):
        return self.account if account_uuid == self.account_uuid else None

    def account_settings(self, account_uuid):
        resource = self.account_resource(account_uuid)
        return None if resource is None else resource["settings"]

    def assignment_for_provider_chat(self, account_uuid, provider_chat_key):
        if provider_chat_key in self.assignments:
            return self.assignments[provider_chat_key]
        if self.account["settings"]["selection_mode"] == "all":
            return {"selected": True, "project_id": self.project_uuid}
        return None

    def producer_lane_position(self, operation_uuid, origin, causal_lane):
        lane = self.positions.setdefault(causal_lane, [])
        if operation_uuid not in lane:
            lane.append(operation_uuid)
        index = lane.index(operation_uuid)
        return index + 1, None if index == 0 else lane[index - 1]

    def pending_provider_message_context(self, account_uuid, workspace_uuid):
        return self.pending_contexts.get((account_uuid, workspace_uuid))

    def provider_mapping(self, account_uuid, entity_kind, provider_id):
        mapping = self.mappings.get((entity_kind, provider_id))
        if mapping is not None or not self.auto_materialize:
            return mapping
        workspace_uuid = converter.stable_entity_uuid(
            self.account_uuid, entity_kind, provider_id
        )
        metadata = {}
        if entity_kind == "identity":
            metadata = {"display_name": f"User {provider_id}", "active": True}
        elif entity_kind == "stream":
            chat_type, _, raw_participants = provider_id.partition(":")
            participant_ids = (
                raw_participants.split(",")
                if chat_type in {"direct", "group_direct"}
                else ["2", "3", "4"]
            )
            metadata = {
                "chat_type": chat_type,
                "project_uuid": self.project_uuid,
                "participants": [
                    self.owner_uuid,
                    *[
                        converter.stable_entity_uuid(
                            self.account_uuid, "identity", value
                        )
                        for value in participant_ids
                        if value != "1"
                    ],
                ],
                "name": "Engineering",
                "description": "",
                "private": True,
                "default_topic_uuid": None,
            }
        elif entity_kind == "topic":
            metadata = {"chat_key": "channel:42"}
        else:
            return None
        mapping = {
            "workspace_uuid": workspace_uuid,
            "provider_id": provider_id,
            "provider_revision": None,
            "metadata": metadata,
            "convergent_alias": False,
        }
        self.mappings[(entity_kind, provider_id)] = mapping
        return mapping

    def provider_mapping_by_name(self, account_uuid, entity_kind, name):
        for (kind, _provider_id), mapping in self.mappings.items():
            metadata = mapping.get("metadata", {})
            if (
                kind == entity_kind
                and str(metadata.get("name", "")).casefold() == name.casefold()
                and (entity_kind != "stream" or metadata.get("chat_type") == "channel")
            ):
                return mapping
        return None

    def workspace_mapping(self, account_uuid, entity_kind, workspace_uuid):
        return next(
            (
                mapping
                for (kind, _provider_id), mapping in self.mappings.items()
                if kind == entity_kind and mapping["workspace_uuid"] == workspace_uuid
            ),
            None,
        )

    def accepted_provider_message_context(self, account_uuid, queue_id, event_id):
        return self.accepted_contexts.get((account_uuid, queue_id, event_id))

    def remember_provider_mapping(
        self,
        account_uuid,
        entity_kind,
        provider_id,
        workspace_uuid,
        metadata,
        provider_revision=None,
    ):
        existing = self.mappings.get((entity_kind, provider_id))
        self.mappings[(entity_kind, provider_id)] = {
            "workspace_uuid": (
                workspace_uuid if existing is None else existing["workspace_uuid"]
            ),
            "provider_id": provider_id,
            "provider_revision": provider_revision,
            "metadata": metadata,
            "convergent_alias": (
                False if existing is None else existing.get("convergent_alias", False)
            ),
        }

    def rename_provider_mapping(
        self,
        account_uuid,
        entity_kind,
        old_provider_id,
        new_provider_id,
        metadata,
        provider_revision=None,
    ):
        existing = self.mappings.get((entity_kind, new_provider_id))
        if existing is not None:
            return existing
        mapping = self.mappings.pop((entity_kind, old_provider_id), None)
        if mapping is None:
            return None
        mapping.update(
            provider_id=new_provider_id,
            provider_revision=provider_revision,
            metadata=metadata,
        )
        self.mappings[(entity_kind, new_provider_id)] = mapping
        return mapping

    def plan_reaction_mapping(
        self,
        account_uuid,
        provider_message_id,
        provider_user_id,
        provider_id,
        legacy_provider_id,
        workspace_uuid,
        metadata,
        create_if_missing=True,
    ):
        prefix = f"{provider_message_id}:{provider_user_id}:"
        reaction_type = str(metadata["reaction_type"])
        emoji_code = str(metadata["emoji_code"]).lower()
        if reaction_type == "unicode_emoji":
            emoji_code = converter.emoji.canonical_unicode_emoji_code(emoji_code)

        def same_reaction(item):
            (entity_kind, current_provider_id), mapping = item
            if entity_kind != "reaction" or not current_provider_id.startswith(prefix):
                return False
            if current_provider_id in {provider_id, legacy_provider_id}:
                return True
            current_metadata = mapping["metadata"]
            if str(current_metadata.get("reaction_type")) != reaction_type:
                return False
            current_code = str(current_metadata.get("emoji_code", "")).lower()
            if reaction_type == "unicode_emoji":
                try:
                    current_code = converter.emoji.canonical_unicode_emoji_code(
                        current_code
                    )
                except ValueError:
                    return False
            return current_code == emoji_code

        candidates = [item for item in self.mappings.items() if same_reaction(item)]
        active = [item for item in candidates if not item[1].get("deleted", False)]
        survivor_item = next(
            (item for item in active if item[0][1] == provider_id),
            active[0] if active else None,
        )
        if survivor_item is None and create_if_missing:
            survivor_item = next(
                (item for item in candidates if item[0][1] == provider_id),
                candidates[0] if candidates else None,
            )
        displaced = []
        if survivor_item is None:
            if not create_if_missing:
                return None, []
            survivor = {
                "workspace_uuid": workspace_uuid,
                "provider_id": provider_id,
                "provider_revision": None,
                "metadata": metadata,
                "convergent_alias": False,
            }
        else:
            survivor = survivor_item[1]
            for _key, mapping in candidates:
                if mapping["workspace_uuid"] != survivor["workspace_uuid"]:
                    displaced.append(mapping)
        return survivor, displaced

    def mark_provider_mapping_deleted(self, account_uuid, entity_kind, provider_id):
        self.mappings.pop((entity_kind, provider_id), None)


def _dm_message(message_id=501):
    return {
        "id": message_id,
        "type": "private",
        "display_recipient": [
            {
                "id": 1,
                "is_me": True,
                "full_name": "Owner",
                "email": "owner@example.invalid",
            },
            {
                "id": 2,
                "is_me": False,
                "full_name": "Other User",
                "email": "other@example.invalid",
            },
        ],
        "sender_id": 2,
        "sender_full_name": "Other User",
        "sender_email": "other@example.invalid",
        "is_me_message": False,
        "recipient_display_name": "Owner, Other User",
        "subject": "",
        "timestamp": 1_700_000_000,
        "content": (
            "@**Other User** see [report.pdf](/user_uploads/a/report.pdf)\n"
            "~~~ quote\nquoted\n~~~"
        ),
    }


def _stream_message(message_id=601, subject="Topic"):
    return {
        "id": message_id,
        "type": "stream",
        "stream_id": 42,
        "display_recipient": "Engineering",
        "sender_id": 2,
        "sender_full_name": "Other User",
        "sender_email": "other@example.invalid",
        "subject": subject,
        "timestamp": 1_700_000_000,
        "content": "hello",
    }


def _operations(records):
    return [record["operation"] for record in records]


def test_dm_conversion_has_owner_membership_identity_urn_and_copied_file():
    store = FakeStore()
    event = {"id": 10, "type": "message", "message": _dm_message()}
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        event,
        original_url="https://chat.example.invalid",
        file_resolver=lambda url, name: "urn:file:00000000-0000-0000-0000-000000000001",
    )
    operations = _operations(records)
    message = next(op for op in operations if op["kind"] == "message.create")
    topic = next(op for op in operations if op["kind"] == "topic.upsert")
    participants = store.mappings[("stream", "direct:1,2")]["metadata"]["participants"]
    assert len(participants) == 2
    assert OWNER_UUID in participants
    assert message["actor_uuid"] != OWNER_UUID
    markdown = message["payload"]["payload"]["content"]
    assert "urn:user:" in markdown
    assert "urn:file:" in markdown
    assert "/user_uploads/" not in markdown
    assert "> quoted" in markdown
    assert topic["payload"]["name"] == "Zulip"


def test_unavailable_historical_file_is_replaced_with_visible_marker():
    original_url = "https://chat.example.invalid/#narrow/near/501"
    converted, lossy = converter.convert_markdown(
        "See [archive.pdf](/user_uploads/a/archive.pdf)",
        {},
        original_url,
        file_resolver=lambda _url, _name: None,
    )

    assert lossy
    assert converted == (
        "See **File unavailable:** archive.pdf\n\n"
        f"[Open original](urn:url:{original_url})"
    )
    assert "/user_uploads/" not in converted


def test_lossy_markdown_without_original_url_does_not_add_empty_link():
    converted, lossy = converter.convert_markdown("@**Unknown User**", {}, "")

    assert lossy
    assert converted == "@Unknown User"
    assert "[Open original]" not in converted


def test_markdown_at_workspace_limit_is_preserved():
    content = "x" * converter.WORKSPACE_MARKDOWN_MAX_LENGTH

    converted, lossy = converter.convert_markdown(content, {}, "")

    assert converted == content
    assert not lossy


def test_markdown_over_workspace_limit_is_truncated_with_marker():
    content = "x" * (converter.WORKSPACE_MARKDOWN_MAX_LENGTH + 1)

    converted, lossy = converter.convert_markdown(content, {}, "")

    assert len(converted) == converter.WORKSPACE_MARKDOWN_MAX_LENGTH
    assert converted.endswith(converter.WORKSPACE_MARKDOWN_TRUNCATION_MARKER)
    assert lossy


def test_truncated_markdown_links_to_original_message():
    content = "x" * (converter.WORKSPACE_MARKDOWN_MAX_LENGTH + 1)
    original_url = "https://chat.example.invalid/#narrow/near/601"

    converted, lossy = converter.convert_markdown(content, {}, original_url)

    assert len(converted) == converter.WORKSPACE_MARKDOWN_MAX_LENGTH
    assert converted.endswith(
        f"[Message truncated; open original](urn:url:{original_url})"
    )
    assert lossy


def test_backfill_message_over_workspace_limit_is_truncated():
    store = FakeStore()
    message = _stream_message()
    message["content"] = "x" * (converter.WORKSPACE_MARKDOWN_MAX_LENGTH + 48)

    operations = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "history",
            {"id": 10, "type": "message", "message": message},
            delivery_class="backfill",
        )
    )

    created = next(
        operation for operation in operations if operation["kind"] == "message.create"
    )
    converted = created["payload"]["payload"]["content"]
    assert len(converted) == converter.WORKSPACE_MARKDOWN_MAX_LENGTH
    assert converted.endswith(converter.WORKSPACE_MARKDOWN_TRUNCATION_MARKER)
    assert created["extensions"]["lossy_conversion"] is True


def test_zulip_internal_and_external_links_become_workspace_urns():
    store = FakeStore()
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "identity",
        "1",
        OWNER_UUID,
        {"display_name": "Owner", "active": True},
    )
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 8, "type": "message", "message": _dm_message(501)},
        original_url="https://chat.example.invalid",
    )
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 9, "type": "message", "message": _stream_message(601)},
        original_url="https://chat.example.invalid",
    )
    linked = _stream_message(602)
    linked["content"] = "\n".join(
        (
            "#**Engineering**",
            "#**Engineering>Topic**",
            "#**Engineering>Topic@601**",
            "[message](https://chat.example.invalid/#narrow/channel/42-Engineering/topic/Topic/near/601)",
            "[stable topic](#narrow/channel/42-Engineering/topic/Topic/with/601)",
            "[dm](https://chat.example.invalid/#narrow/dm/2-Other-User)",
            "[profile](https://chat.example.invalid/#user/2)",
            "[site](https://example.com/a?x=1#section)",
            "Plain https://chat.example.invalid/#narrow/channel/42-Engineering/topic/Topic/near/601",
            "[external narrow](//evil.example/#narrow/channel/42-Engineering/topic/Topic/near/601)",
            "[help](/help/link-to-a-message-or-conversation)",
            "Bare https://example.org/docs.",
            "<https://example.edu/autolink>",
            "`https://example.net/code`",
        )
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": linked},
        original_url="https://chat.example.invalid",
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )
    markdown = message["payload"]["payload"]["content"]
    stream_uuid = store.mappings[("stream", "channel:42")]["workspace_uuid"]
    topic_uuid = store.mappings[("topic", "42:Topic")]["workspace_uuid"]
    message_uuid = store.mappings[("message", "601")]["workspace_uuid"]
    dm_uuid = store.mappings[("stream", "direct:1,2")]["workspace_uuid"]
    user_uuid = store.mappings[("identity", "2")]["workspace_uuid"]

    assert f"[#Engineering](urn:stream:{stream_uuid})" in markdown
    assert f"[#Engineering > Topic](urn:topic:{topic_uuid})" in markdown
    assert f"[#Engineering > Topic @ 💬](urn:message:{message_uuid})" in markdown
    assert f"[message](urn:message:{message_uuid})" in markdown
    assert f"[stable topic](urn:topic:{topic_uuid})" in markdown
    assert f"[dm](urn:stream:{dm_uuid})" in markdown
    assert f"[profile](urn:user:{user_uuid})" in markdown
    assert "[site](urn:url:https://example.com/a?x=1#section)" in markdown
    assert (
        "[https://chat.example.invalid/#narrow/channel/42-Engineering/topic/Topic/"
        f"near/601](urn:message:{message_uuid})" in markdown
    )
    assert (
        "[external narrow](urn:url:https://evil.example/#narrow/channel/"
        "42-Engineering/topic/Topic/near/601)" in markdown
    )
    assert (
        "[help](urn:url:https://chat.example.invalid/help/"
        "link-to-a-message-or-conversation)" in markdown
    )
    assert "[https://example.org/docs](urn:url:https://example.org/docs)." in markdown
    assert (
        "[https://example.edu/autolink](urn:url:https://example.edu/autolink)"
        in markdown
    )
    assert "[[" not in markdown
    assert "`https://example.net/code`" in markdown
    assert "#**" not in markdown
    assert "Open original" not in markdown


@pytest.mark.parametrize(
    "malformed_url",
    (
        "https://[2001:db8::1",
        "http://[::1",
    ),
)
def test_malformed_ipv6_bare_url_is_preserved_without_aborting_conversion(
    malformed_url,
):
    store = FakeStore()
    resolver = converter.ZulipLinkResolver(store, ACCOUNT_UUID, OWNER_UUID)
    original_url = "https://chat.example.invalid/#narrow/near/602"

    converted, lossy = converter.convert_markdown(
        f"Broken link: {malformed_url}",
        {},
        original_url,
        link_resolver=resolver,
    )

    assert not lossy
    assert converted == f"Broken link: {malformed_url}"


@pytest.mark.parametrize(
    "malformed_link",
    (
        "<https://[2001:db8::1>",
        "[label](https://[2001:db8::1)",
    ),
)
def test_malformed_ipv6_link_markup_is_preserved(malformed_link):
    store = FakeStore()
    resolver = converter.ZulipLinkResolver(store, ACCOUNT_UUID, OWNER_UUID)
    content = f"Broken link: {malformed_link}"

    converted, lossy = converter.convert_markdown(
        content,
        {},
        "https://chat.example.invalid/#narrow/near/602",
        link_resolver=resolver,
    )

    assert not lossy
    assert converted == content


def test_valid_ipv6_bare_url_remains_clickable():
    store = FakeStore()
    resolver = converter.ZulipLinkResolver(store, ACCOUNT_UUID, OWNER_UUID)
    ipv6_url = "https://[2001:db8::1]/docs"

    converted, lossy = converter.convert_markdown(
        ipv6_url,
        {},
        "https://chat.example.invalid/#narrow/near/602",
        link_resolver=resolver,
    )

    assert not lossy
    assert converted == f"[{ipv6_url}](urn:url:{ipv6_url})"


def test_malformed_ipv6_semantic_reply_url_is_ignored():
    malformed_url = "https://[2001:db8::1"
    link = markdown_conversion.MarkdownLink(
        raw=f"[Quoted message]({malformed_url})",
        image=False,
        label="Quoted message",
        destination="https://:1%5B2001:db8:",
        destination_prefix="[Quoted message](",
        destination_suffix=")",
    )

    assert converter._reply_provider_id(link) is None


def test_zulip_quote_fences_convert_prose_links_and_preserve_nested_code():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 9, "type": "message", "message": _stream_message(601)},
        original_url="https://chat.example.invalid",
    )
    resolver = converter.ZulipLinkResolver(store, ACCOUNT_UUID, OWNER_UUID)
    stream_uuid = store.mappings[("stream", "channel:42")]["workspace_uuid"]
    topic_uuid = store.mappings[("topic", "42:Topic")]["workspace_uuid"]
    message_uuid = store.mappings[("message", "601")]["workspace_uuid"]
    content = "\n".join(
        (
            "````quote",
            "[docs](https://example.com/a?x=1#section)",
            "#**Engineering**",
            "#**Engineering>Topic**",
            "#**Engineering>Topic@601**",
            "```text",
            "https://example.net/code",
            "#**Engineering**",
            "```",
            "````",
        )
    )

    converted, lossy = converter.convert_markdown(
        content,
        {},
        "https://chat.example.invalid/#narrow/near/602",
        link_resolver=resolver,
    )

    assert not lossy
    assert "> [docs](urn:url:https://example.com/a?x=1#section)" in converted
    assert f"> [#Engineering](urn:stream:{stream_uuid})" in converted
    assert f"> [#Engineering > Topic](urn:topic:{topic_uuid})" in converted
    assert f"> [#Engineering > Topic @ 💬](urn:message:{message_uuid})" in converted
    assert (
        "> ```text\n> https://example.net/code\n> #**Engineering**\n> ```" in converted
    )
    assert "urn:url:https://example.net/code" not in converted


def test_zulip_triple_backtick_quote_converts_to_workspace_blockquote():
    converted, lossy = converter.convert_markdown(
        "```quote\n[docs](https://example.com/reference)\n```",
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == "> [docs](urn:url:https://example.com/reference)"


def test_zulip_quote_closing_fence_may_be_longer_than_opener():
    converted, lossy = converter.convert_markdown(
        "````quote\n[docs](https://example.com/reference)\n`````",
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == "> [docs](urn:url:https://example.com/reference)"


def test_zulip_quote_with_crlf_line_endings_is_converted():
    converted, lossy = converter.convert_markdown(
        "```quote\r\n[docs](https://example.com/reference)\r\n```\r\nnext",
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == ("> [docs](urn:url:https://example.com/reference)\r\nnext")


def test_zulip_quote_inside_outer_fence_remains_literal_code():
    content = (
        "`````markdown\n```quote\n[docs](https://example.com/reference)\n```\n`````"
    )

    converted, lossy = converter.convert_markdown(
        content,
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == content


def test_zulip_quote_preserves_list_item_indentation():
    converted, lossy = converter.convert_markdown(
        "- item\n  ```quote\n  [docs](https://example.com/reference)\n  ```",
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == ("- item\n  > [docs](urn:url:https://example.com/reference)")


def test_four_space_indented_quote_fence_remains_literal_code():
    content = "    ```quote\n    [docs](https://example.com/reference)\n    ```"

    converted, lossy = converter.convert_markdown(
        content,
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == content


def test_four_space_indented_fence_does_not_close_top_level_quote():
    content = "```quote\n[docs](https://example.com/reference)\n    ```"

    converted, lossy = converter.convert_markdown(
        content,
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == content


def test_zulip_link_title_is_preserved_during_urn_conversion():
    converted, lossy = converter.convert_markdown(
        '[docs](https://example.com/reference "Documentation")',
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == (
        '[docs](urn:url:https://example.com/reference "Documentation")'
    )


def test_commonmark_reference_link_becomes_inline_workspace_link():
    converted, lossy = converter.convert_markdown(
        '[docs][reference]\n\n[reference]: https://example.com/reference "Docs"',
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == ('[docs](urn:url:https://example.com/reference "Docs")\n\n')


@pytest.mark.parametrize(
    "literal_code",
    (
        "`[docs][reference]`",
        "```markdown\n[docs][reference]\n```",
    ),
)
def test_reference_definition_is_kept_when_used_by_literal_code(
    literal_code,
):
    content = (
        "[docs][reference]\n\n"
        f"{literal_code}\n\n"
        '[reference]: https://example.com/reference "Docs"'
    )

    converted, lossy = converter.convert_markdown(
        content,
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == content.replace(
        "[docs][reference]",
        '[docs](urn:url:https://example.com/reference "Docs")',
        1,
    )


def test_multiline_commonmark_link_preserves_container_prefixes():
    converted, lossy = converter.convert_markdown(
        '> [docs](\n>   https://example.com/reference\n>   "Documentation"\n> )',
        {},
        "https://chat.example.invalid/#narrow/near/602",
    )

    assert not lossy
    assert converted == (
        '> [docs](\n>   urn:url:https://example.com/reference\n>   "Documentation"\n> )'
    )


def test_unresolved_native_zulip_link_is_preserved_and_marks_conversion_lossy():
    store = FakeStore(auto_materialize=False)
    resolver = converter.ZulipLinkResolver(store, ACCOUNT_UUID, OWNER_UUID)

    converted, lossy = converter.convert_markdown(
        "See #**Unknown>missing**",
        {},
        "https://chat.example.invalid/#narrow/near/700",
        link_resolver=resolver,
    )

    assert lossy
    assert "#**Unknown>missing**" in converted
    assert (
        "[Open original](urn:url:https://chat.example.invalid/#narrow/near/700)"
        in converted
    )


def test_new_chat_waits_for_backend_assignment_before_materialization():
    event = {"id": 10, "type": "message", "message": _stream_message()}
    with pytest.raises(ValueError, match="provider_chat_not_selected"):
        converter.event_records(FakeStore("explicit"), ACCOUNT_UUID, "queue", event)
    pending = FakeStore("all")
    pending.assignment_for_provider_chat = lambda *args: None
    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(pending, ACCOUNT_UUID, "queue", event)
    materialized = FakeStore("all")
    operations = _operations(
        converter.event_records(materialized, ACCOUNT_UUID, "queue", event)
    )
    assert any(op["kind"] == "message.create" for op in operations)
    assert any(op["kind"] == "topic.upsert" for op in operations)
    assert not any(op["kind"] == "stream.upsert" for op in operations)
    participants = materialized.mappings[("stream", "channel:42")]["metadata"][
        "participants"
    ]
    assert OWNER_UUID in participants
    assert converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "2") in participants


def test_channel_message_waits_for_topic_and_accepts_former_author():
    store = FakeStore(auto_materialize=False)
    stream_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    event = {"id": 10, "type": "message", "message": _stream_message()}
    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    topic_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:Topic",
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
    )
    operations = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    )
    author_uuid = converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "2")
    message = next(value for value in operations if value["kind"] == "message.create")
    identity = next(value for value in operations if value["kind"] == "identity.upsert")
    topic = next(value for value in operations if value["kind"] == "topic.upsert")
    assert message["actor_uuid"] == author_uuid
    assert identity["entity_uuid"] == author_uuid
    assert message["payload"]["topic_uuid"] == topic_uuid
    assert topic["entity_uuid"] == topic_uuid
    assert store.mappings[("stream", "channel:42")]["metadata"]["participants"] == [
        OWNER_UUID
    ]
    assert not any(value["kind"] == "stream.upsert" for value in operations)


def test_channel_message_skips_already_committed_topic_projection():
    store = FakeStore()
    first = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message(601)},
    )
    assert any(operation["kind"] == "topic.upsert" for operation in _operations(first))
    store.mappings[("topic", "42:Topic")]["metadata"]["workspace_delivery_state"] = (
        "committed"
    )

    second = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": _stream_message(602)},
    )
    operations = _operations(second)

    assert [operation["kind"] for operation in operations] == ["message.create"]


@pytest.mark.parametrize("subject", ["", "general chat", "General Chat"])
def test_empty_channel_topic_uses_backend_owned_default_topic(subject):
    store = FakeStore(auto_materialize=False)
    stream_uuid = str(uuid.uuid4())
    default_topic_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": default_topic_uuid,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:general chat",
        default_topic_uuid,
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "general chat",
            "is_default": True,
        },
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message(subject=subject)},
    )
    operations = _operations(records)
    message = next(value for value in operations if value["kind"] == "message.create")
    topic = next(value for value in operations if value["kind"] == "topic.upsert")

    assert message["payload"]["topic_uuid"] == default_topic_uuid
    assert topic["entity_uuid"] == default_topic_uuid
    assert topic["payload"] == {
        "stream_uuid": stream_uuid,
        "name": "general chat",
    }
    assert store.mappings[("topic", "42:general chat")]["workspace_uuid"] == (
        default_topic_uuid
    )
    assert store.mappings[("message", "601")]["metadata"]["subject"] == ("general chat")


def test_empty_channel_topic_waits_for_backend_default_topic_assignment():
    store = FakeStore(auto_materialize=False)
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:general chat",
        topic_uuid,
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "general chat",
            "is_default": True,
        },
    )

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": _stream_message(subject="")},
        )


def test_empty_channel_topic_waits_when_provider_mapping_disagrees_with_default():
    store = FakeStore(auto_materialize=False)
    stream_uuid = str(uuid.uuid4())
    stale_default_topic_uuid = str(uuid.uuid4())
    provider_topic_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": stale_default_topic_uuid,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:general chat",
        provider_topic_uuid,
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "general chat",
            "is_default": True,
        },
    )

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": _stream_message(subject="")},
        )


def test_empty_channel_topic_reconciles_disagreeing_projection():
    provider_topic_uuid = str(uuid.uuid4())

    class ReconcilingStore(FakeStore):
        def __init__(self):
            super().__init__(auto_materialize=False)
            self.reconciled = 0

        def reconcile_assignment_projection(self, account_uuid, provider_chat_key):
            assert account_uuid == ACCOUNT_UUID
            assert provider_chat_key == "channel:42"
            self.reconciled += 1
            stream = self.mappings[("stream", provider_chat_key)]
            stream["metadata"]["default_topic_uuid"] = provider_topic_uuid
            return True

    store = ReconcilingStore()
    stream_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": str(uuid.uuid4()),
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:general chat",
        provider_topic_uuid,
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "general chat",
            "is_default": True,
        },
    )

    operations = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": _stream_message(subject="")},
        )
    )

    topic = next(value for value in operations if value["kind"] == "topic.upsert")
    assert store.reconciled == 1
    assert topic["entity_uuid"] == provider_topic_uuid


def test_message_mutations_and_topic_rename_reuse_stable_mappings():
    store = FakeStore()
    create = {"id": 10, "type": "message", "message": _stream_message()}
    created = converter.event_records(store, ACCOUNT_UUID, "queue", create)
    created_message = next(
        operation
        for operation in _operations(created)
        if operation["kind"] == "message.create"
    )
    original_content_sha256 = store.provider_mapping(ACCOUNT_UUID, "message", "601")[
        "metadata"
    ]["content_sha256"]
    external_author_uuid = created_message["payload"]["author_uuid"]
    topic_uuid = store.provider_mapping(ACCOUNT_UUID, "topic", "42:Topic")[
        "workspace_uuid"
    ]
    store.auto_materialize = False
    update = {
        "id": 11,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "orig_subject": "Topic",
        "subject": "Renamed",
        "content": "edited",
        "edit_timestamp": 1_700_000_010,
    }
    updated = _operations(converter.event_records(store, ACCOUNT_UUID, "queue", update))
    assert [operation["kind"] for operation in updated] == [
        "topic.upsert",
        "message.update",
    ]
    assert updated[0]["entity_uuid"] == topic_uuid
    assert updated[1]["actor_uuid"] == external_author_uuid
    assert (
        store.provider_mapping(ACCOUNT_UUID, "message", "601")["metadata"][
            "content_sha256"
        ]
        == original_content_sha256
    )
    assert updated[1]["extensions"]["content_sha256"] != original_content_sha256
    deleted = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 12, "type": "delete_message", "message_ids": [601]},
        )
    )
    assert deleted[0]["kind"] == "message.delete"
    assert deleted[0]["actor_uuid"] == external_author_uuid
    assert deleted[0]["payload"]["author_uuid"] == external_author_uuid
    # Conversion is side-effect free for deletion. The service tombstones the
    # mapping atomically with the provider journal after durable enqueue.
    assert store.provider_mapping(ACCOUNT_UUID, "message", "601") is not None

    converter.event_records(store, ACCOUNT_UUID, "queue", create)
    read = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 13,
                "type": "update_message_flags",
                "flag": "read",
                "op": "add",
                "messages": [601],
            },
        )
    )
    assert read[0]["kind"] == "read_state.set"
    assert read[0]["payload"]["reader_uuid"] == OWNER_UUID
    assert read[0]["payload"]["message_uuids"] == [created_message["entity_uuid"]]
    assert "through_message_uuid" not in read[0]["payload"]


def test_repeated_topic_rename_reuses_existing_target_mapping():
    store = FakeStore()
    stream_uuid = str(uuid.uuid4())
    old_topic_uuid = str(uuid.uuid4())
    target_topic_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    topic_metadata = {
        "stream_uuid": stream_uuid,
        "chat_key": "channel:42",
        "is_default": False,
    }
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:TopicA",
        old_topic_uuid,
        {**topic_metadata, "name": "TopicA"},
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:TopicB",
        target_topic_uuid,
        {**topic_metadata, "name": "TopicB"},
    )
    message_uuid = str(uuid.uuid4())
    author_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "message",
        "601",
        message_uuid,
        {
            "project_uuid": PROJECT_UUID,
            "stream_uuid": stream_uuid,
            "topic_uuid": old_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
        },
    )

    operations = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 303,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 42,
                "orig_subject": "TopicA",
                "subject": "TopicB",
                "propagate_mode": "change_one",
                "edit_timestamp": 1_700_000_010,
            },
        )
    )

    assert [operation["kind"] for operation in operations] == [
        "topic.upsert",
        "message.update",
    ]
    assert operations[0]["entity_uuid"] == target_topic_uuid
    assert operations[1]["entity_uuid"] == message_uuid
    assert operations[1]["payload"]["topic_uuid"] == target_topic_uuid
    assert "payload" not in operations[1]["payload"]
    assert store.provider_mapping(ACCOUNT_UUID, "topic", "42:TopicA") is not None
    assert (
        store.provider_mapping(ACCOUNT_UUID, "topic", "42:TopicB")["workspace_uuid"]
        == target_topic_uuid
    )


def test_single_message_topic_move_creates_target_without_renaming_source():
    store = FakeStore()
    created = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": _stream_message()},
        )
    )
    created_message = next(
        operation for operation in created if operation["kind"] == "message.create"
    )
    source_topic_uuid = created_message["payload"]["topic_uuid"]
    store.auto_materialize = False

    moved = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 42,
                "orig_subject": "Topic",
                "subject": "TopicB",
                "propagate_mode": "change_one",
                "edit_timestamp": 1_700_000_010,
            },
        )
    )

    target_topic = store.provider_mapping(ACCOUNT_UUID, "topic", "42:TopicB")
    assert target_topic is not None
    assert target_topic["workspace_uuid"] != source_topic_uuid
    assert store.provider_mapping(ACCOUNT_UUID, "topic", "42:Topic") is not None
    assert [operation["kind"] for operation in moved] == [
        "topic.upsert",
        "message.update",
    ]
    assert moved[1]["payload"]["topic_uuid"] == target_topic["workspace_uuid"]


def test_combined_topic_move_applies_content_only_to_edited_message():
    store = FakeStore()
    for message_id in (601, 602):
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": message_id,
                "type": "message",
                "message": _stream_message(message_id),
            },
        )
    store.auto_materialize = False

    operations = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 700,
                "type": "update_message",
                "message_id": 602,
                "message_ids": [601, 602],
                "stream_id": 42,
                "orig_subject": "Topic",
                "subject": "Renamed",
                "propagate_mode": "change_all",
                "content": "edited only once",
                "edit_timestamp": 1_700_000_010,
            },
        )
    )

    message_updates = [
        operation for operation in operations if operation["kind"] == "message.update"
    ]
    assert len(message_updates) == 2
    by_provider_id = {
        operation["provider"]["entity_id"]: operation for operation in message_updates
    }
    assert "payload" not in by_provider_id["601"]["payload"]
    assert by_provider_id["602"]["payload"]["payload"] == {
        "kind": "markdown",
        "content": "edited only once",
    }


def test_channel_move_uses_destination_stream_and_topic_with_unchanged_subject():
    store = FakeStore()
    created = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": _stream_message()},
        )
    )
    created_message = next(
        operation for operation in created if operation["kind"] == "message.create"
    )
    source_topic_uuid = created_message["payload"]["topic_uuid"]
    destination_stream_uuid = str(uuid.uuid4())
    destination_topic_uuid = str(uuid.uuid4())
    store.assignments["channel:43"] = {
        "selected": True,
        "project_id": PROJECT_UUID,
    }
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:43",
        destination_stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Operations",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "43:Topic",
        destination_topic_uuid,
        {
            "stream_uuid": destination_stream_uuid,
            "chat_key": "channel:43",
            "is_default": False,
            "name": "Topic",
        },
    )
    store.auto_materialize = False

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "new_stream_id": 43,
            "orig_subject": "Topic",
            "subject": "Topic",
            "propagate_mode": "change_one",
            "edit_timestamp": 1_700_000_010,
        },
    )
    operations = _operations(records)

    assert [operation["kind"] for operation in operations] == [
        "topic.upsert",
        "message.update",
    ]
    assert operations[0]["payload"]["stream_uuid"] == destination_stream_uuid
    assert operations[1]["payload"]["stream_uuid"] == destination_stream_uuid
    assert operations[1]["payload"]["topic_uuid"] == destination_topic_uuid
    assert operations[1]["provider"]["chat_id"] == "channel:43"
    assert records[0]["project_uuid"] == PROJECT_UUID
    assert records[1]["project_uuid"] == PROJECT_UUID
    assert (
        store.provider_mapping(ACCOUNT_UUID, "topic", "42:Topic")["workspace_uuid"]
        == source_topic_uuid
    )


def test_channel_move_and_followup_edit_keep_original_message_lane_and_destination():
    store = FakeStore()
    created_records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )
    created_message = next(
        record
        for record in created_records
        if record["operation"]["kind"] == "message.create"
    )
    message_uuid = str(created_message["operation"]["entity_uuid"])
    destination_project_uuid = str(uuid.uuid4())
    destination_stream_uuid = str(uuid.uuid4())
    destination_topic_uuid = str(uuid.uuid4())
    store.assignments["channel:43"] = {
        "selected": True,
        "project_id": destination_project_uuid,
    }
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:43",
        destination_stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": destination_project_uuid,
            "participants": [OWNER_UUID],
            "name": "Operations",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "43:Topic",
        destination_topic_uuid,
        {
            "stream_uuid": destination_stream_uuid,
            "chat_key": "channel:43",
            "is_default": False,
            "name": "Topic",
        },
    )
    store.auto_materialize = False

    move_records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "new_stream_id": 43,
            "orig_subject": "Topic",
            "subject": "Topic",
            "propagate_mode": "change_one",
            "edit_timestamp": 1_700_000_010,
        },
    )
    moved_message = next(
        record
        for record in move_records
        if record["operation"]["kind"] == "message.update"
    )
    store.pending_contexts[(ACCOUNT_UUID, message_uuid)] = {
        "project_uuid": destination_project_uuid,
        "stream_uuid": destination_stream_uuid,
        "topic_uuid": destination_topic_uuid,
        "chat_key": "channel:43",
        "causal_lane": moved_message["causal_lane"],
        "subject": "Topic",
    }

    edit_records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 12,
            "type": "update_message",
            "message_id": 601,
            "stream_id": 43,
            "content": "edited after move",
            "edit_timestamp": 1_700_000_011,
        },
    )
    edited_message = next(
        record
        for record in edit_records
        if record["operation"]["kind"] == "message.update"
    )

    assert moved_message["causal_lane"] == created_message["causal_lane"]
    assert edited_message["causal_lane"] == created_message["causal_lane"]
    assert edited_message["project_uuid"] == destination_project_uuid
    assert edited_message["operation"]["provider"]["chat_id"] == "channel:43"
    assert edited_message["operation"]["payload"]["stream_uuid"] == (
        destination_stream_uuid
    )
    assert edited_message["operation"]["payload"]["topic_uuid"] == (
        destination_topic_uuid
    )


def test_channel_move_to_unselected_destination_deletes_source_projection():
    store = FakeStore(selection_mode="manual")
    store.assignments["channel:42"] = {
        "selected": True,
        "project_id": PROJECT_UUID,
    }
    store.assignments["channel:43"] = {
        "selected": False,
        "project_id": PROJECT_UUID,
    }
    created_records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )
    created_message = next(
        record
        for record in created_records
        if record["operation"]["kind"] == "message.create"
    )
    store.auto_materialize = False

    moved_records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "new_stream_id": 43,
            "orig_subject": "Topic",
            "subject": "Topic",
            "propagate_mode": "change_one",
            "edit_timestamp": 1_700_000_010,
        },
    )

    assert [record["operation"]["kind"] for record in moved_records] == [
        "message.delete"
    ]
    assert moved_records[0]["project_uuid"] == PROJECT_UUID
    assert moved_records[0]["operation"]["provider"]["chat_id"] == "channel:42"
    assert moved_records[0]["causal_lane"] == created_message["causal_lane"]


def test_content_only_message_update_does_not_infer_a_topic_name():
    store = FakeStore(auto_materialize=False)
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    author_uuid = str(uuid.uuid4())
    message_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "message",
        "601",
        message_uuid,
        {
            "project_uuid": PROJECT_UUID,
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "mapping_origin": "workspace",
            "workspace_delivery_state": "committed",
        },
    )

    updated = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 42,
                "content": "edited",
                "edit_timestamp": 1_700_000_010,
            },
        )
    )

    assert [operation["kind"] for operation in updated] == ["message.update"]
    assert updated[0]["entity_uuid"] == message_uuid
    assert updated[0]["payload"]["topic_uuid"] == topic_uuid
    assert "subject" not in updated[0]["extensions"]
    assert ("topic", "42:general chat") not in store.mappings


def test_read_state_drops_retired_stream_mapping_after_recanonicalization():
    store = FakeStore()
    first_event = {
        "id": 10,
        "type": "message",
        "message": _stream_message(601, "Topic"),
    }
    first_records = converter.event_records(store, ACCOUNT_UUID, "queue", first_event)
    first_message = next(
        operation
        for operation in _operations(first_records)
        if operation["kind"] == "message.create"
    )
    stream_mapping = store.mappings[("stream", "channel:42")]
    stream_mapping["workspace_uuid"] = str(uuid.uuid4())

    second_event = {
        "id": 11,
        "type": "message",
        "message": _stream_message(602, "Topic"),
    }
    second_records = converter.event_records(store, ACCOUNT_UUID, "queue", second_event)
    second_message = next(
        operation
        for operation in _operations(second_records)
        if operation["kind"] == "message.create"
    )

    read = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 12,
                "type": "update_message_flags",
                "flag": "read",
                "op": "add",
                "messages": [601, 602],
            },
        )
    )

    assert len(read) == 1
    assert read[0]["kind"] == "read_state.set"
    assert read[0]["payload"]["stream_uuid"] == stream_mapping["workspace_uuid"]
    assert read[0]["payload"]["message_uuids"] == [second_message["entity_uuid"]]
    assert first_message["entity_uuid"] not in read[0]["payload"]["message_uuids"]


def test_message_snapshot_and_live_events_project_unicode_reaction_code():
    store = FakeStore()
    message = _stream_message()
    message["reactions"] = [
        {
            "user_id": 3,
            "emoji_name": "thumbs_up",
            "emoji_code": "1f44d",
            "reaction_type": "unicode_emoji",
        }
    ]
    snapshot = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": message},
        )
    )
    snapshot_reaction = next(
        operation for operation in snapshot if operation["kind"] == "reaction.upsert"
    )
    message_operation = next(
        operation for operation in snapshot if operation["kind"] == "message.create"
    )

    assert snapshot.index(message_operation) < snapshot.index(snapshot_reaction)
    assert snapshot_reaction["payload"] == {
        "stream_uuid": message_operation["payload"]["stream_uuid"],
        "topic_uuid": message_operation["payload"]["topic_uuid"],
        "message_uuid": message_operation["entity_uuid"],
        "user_uuid": converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "3"),
        "emoji_name": "👍",
    }
    assert snapshot_reaction["provider"]["entity_id"] == ("601:3:unicode_emoji:1f44d")
    assert snapshot_reaction["extensions"] == {
        "provider_badge": "zulip",
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
        "delivery_class": "live",
    }
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "reaction",
        "601:3:unicode_emoji:1f44d",
        snapshot_reaction["entity_uuid"],
        {
            "emoji_name": "👍",
            "provider_emoji_name": "thumbs_up",
            "emoji_code": "1f44d",
            "reaction_type": "unicode_emoji",
        },
    )

    removed = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "reaction",
                "op": "remove",
                "message_id": 601,
                "user_id": 3,
                "emoji_name": "thumbs_up",
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            },
        )
    )
    removed_reaction = next(
        operation for operation in removed if operation["kind"] == "reaction.delete"
    )

    assert removed_reaction["entity_uuid"] == snapshot_reaction["entity_uuid"]
    assert removed_reaction["payload"] == snapshot_reaction["payload"]


def test_remove_reaction_without_active_mapping_is_ignored():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "reaction",
            "op": "remove",
            "message_id": 601,
            "user_id": 3,
            "emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )

    assert records == []
    assert (
        store.provider_mapping(
            ACCOUNT_UUID,
            "reaction",
            "601:3:unicode_emoji:270d",
        )
        is None
    )


def test_reaction_from_unknown_user_creates_explicit_unavailable_identity():
    class MissingIdentityStore(FakeStore):
        def provider_mapping(self, account_uuid, entity_kind, provider_id):
            if entity_kind == "identity" and provider_id == "99":
                return self.mappings.get((entity_kind, provider_id))
            return super().provider_mapping(account_uuid, entity_kind, provider_id)

    store = MissingIdentityStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )

    operations = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "reaction",
                "op": "add",
                "message_id": 601,
                "user_id": 99,
                "emoji_name": "eyes",
                "emoji_code": "1f440",
                "reaction_type": "unicode_emoji",
            },
        )
    )

    identity = next(
        operation for operation in operations if operation["kind"] == "identity.upsert"
    )
    assert identity["provider"]["entity_id"] == "99"
    assert identity["payload"] == {
        "display_name": "Unavailable Zulip user (ID 99)",
        "email": None,
        "avatar_urn": None,
        "active": False,
    }


def test_reaction_aliases_for_one_unicode_code_share_workspace_identity():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )

    first = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "reaction",
                "op": "add",
                "message_id": 601,
                "user_id": 3,
                "emoji_name": "+1",
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            },
        )
    )
    second = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 12,
                "type": "reaction",
                "op": "add",
                "message_id": 601,
                "user_id": 3,
                "emoji_name": "thumbs_up",
                "emoji_code": "1F44D-FE0F",
                "reaction_type": "unicode_emoji",
            },
        )
    )

    first_reaction = next(
        operation for operation in first if operation["kind"] == "reaction.upsert"
    )
    second_reaction = next(
        operation for operation in second if operation["kind"] == "reaction.upsert"
    )
    assert second_reaction["entity_uuid"] == first_reaction["entity_uuid"]
    assert second_reaction["provider"]["entity_id"] == ("601:3:unicode_emoji:1f44d")
    assert second_reaction["payload"]["emoji_name"] == "👍"
    assert second_reaction["extensions"]["emoji_name"] == "thumbs_up"


def test_legacy_reaction_mapping_cleanup_is_planned_without_early_mutation():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )
    legacy_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "reaction",
        "601:3:writing",
        legacy_uuid,
        {"emoji_name": "writing"},
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "reaction",
            "op": "add",
            "message_id": 601,
            "user_id": 3,
            "emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )
    operations = _operations(records)

    reaction = next(
        operation for operation in operations if operation["kind"] == "reaction.upsert"
    )
    assert reaction["entity_uuid"] == legacy_uuid
    assert reaction["payload"]["emoji_name"] == "✍"
    assert store.provider_mapping(ACCOUNT_UUID, "reaction", "601:3:writing") is not None
    assert (
        store.provider_mapping(ACCOUNT_UUID, "reaction", "601:3:unicode_emoji:270d")
        is None
    )
    reaction_record = next(
        record for record in records if record["operation"]["kind"] == "reaction.upsert"
    )
    plan = reaction_record["transport"]["reaction_mapping"]
    assert plan["workspace_uuid"] == legacy_uuid
    assert plan["provider_id"] == "601:3:unicode_emoji:270d"


def test_partial_reaction_migration_merges_aliases_by_type_and_code():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )
    canonical_uuid = str(uuid.uuid4())
    stale_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "reaction",
        "601:3:unicode_emoji:270d",
        canonical_uuid,
        {
            "emoji_name": "✍",
            "provider_emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "reaction",
        "601:3:writing_hand",
        stale_uuid,
        {
            "emoji_name": "writing_hand",
            "emoji_code": "270D-FE0F",
            "reaction_type": "unicode_emoji",
        },
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "reaction",
            "op": "add",
            "message_id": 601,
            "user_id": 3,
            "emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )
    operations = _operations(records)

    assert [operation["kind"] for operation in operations] == [
        "reaction.delete",
        "reaction.upsert",
    ]
    assert operations[0]["entity_uuid"] == stale_uuid
    assert operations[0]["payload"]["emoji_name"] == "writing_hand"
    assert operations[1]["entity_uuid"] == canonical_uuid
    assert operations[1]["payload"]["emoji_name"] == "✍"
    assert (
        store.provider_mapping(ACCOUNT_UUID, "reaction", "601:3:writing_hand")
        is not None
    )
    reaction_record = next(
        record for record in records if record["operation"]["kind"] == "reaction.upsert"
    )
    plan = reaction_record["transport"]["reaction_mapping"]
    assert plan["workspace_uuid"] == canonical_uuid
    assert plan["displaced"] == [
        {
            "workspace_uuid": stale_uuid,
            "provider_id": "601:3:writing_hand",
        }
    ]


def test_custom_reaction_keeps_provider_name_and_uses_typed_code_identity():
    store = FakeStore()
    message = _stream_message()
    message["reactions"] = [
        {
            "user_id": 3,
            "emoji_name": "party_parrot",
            "emoji_code": "17",
            "reaction_type": "realm_emoji",
        }
    ]

    operations = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 10, "type": "message", "message": message},
        )
    )

    reaction = next(
        operation for operation in operations if operation["kind"] == "reaction.upsert"
    )
    assert reaction["provider"]["entity_id"] == "601:3:realm_emoji:17"
    assert reaction["payload"]["emoji_name"] == "party_parrot"
    assert reaction["extensions"] == {
        "provider_badge": "zulip",
        "emoji_name": "party_parrot",
        "emoji_code": "17",
        "reaction_type": "realm_emoji",
        "delivery_class": "live",
    }


def test_backfill_reaction_uses_versioned_projection_operation_identity():
    store = FakeStore()
    message = _stream_message()
    message["reactions"] = [
        {
            "user_id": 3,
            "emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        }
    ]

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "history",
        {"id": 601, "type": "message", "message": message},
        "backfill",
    )

    reaction = next(
        record for record in records if record["operation"]["kind"] == "reaction.upsert"
    )
    reaction_index = records.index(reaction)
    source = (
        "provider-message:601:reconcile-generation:1:"
        f"reaction-projection:{converter.REACTION_PROJECTION_VERSION}"
    )
    assert reaction["operation_uuid"] == converter.operation_uuid_for(
        ACCOUNT_UUID,
        source,
        601,
        reaction_index,
    )
    message_record = next(
        record for record in records if record["operation"]["kind"] == "message.create"
    )
    assert message_record["operation_uuid"] == converter.operation_uuid_for(
        ACCOUNT_UUID,
        "provider-message:601:reconcile-generation:1",
        601,
        records.index(message_record),
    )


def test_reaction_event_rejects_unknown_operation():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )

    with pytest.raises(ValueError, match="reaction operation is invalid"):
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "reaction",
                "op": "replace",
                "message_id": 601,
                "user_id": 3,
                "emoji_name": "thumbs_up",
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            },
        )


@pytest.mark.parametrize(("flags", "expected_read"), [(["read"], True), ([], False)])
def test_message_snapshot_carries_exact_owner_read_state(flags, expected_read):
    store = FakeStore()
    event = {"id": 10, "type": "message", "message": _stream_message()}
    event["message"]["flags"] = flags

    operations = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    )

    created = next(
        operation for operation in operations if operation["kind"] == "message.create"
    )
    assert created["payload"]["read"] is expected_read
    assert not any(operation["kind"] == "read_state.set" for operation in operations)


@pytest.mark.parametrize(("flags", "expected_read"), [(["read"], True), ([], False)])
def test_live_message_event_carries_top_level_owner_read_state(flags, expected_read):
    store = FakeStore()
    event = {
        "id": 10,
        "type": "message",
        "flags": flags,
        "message": _stream_message(),
    }

    operations = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    )

    created = next(
        operation for operation in operations if operation["kind"] == "message.create"
    )
    assert created["payload"]["read"] is expected_read
    assert not any(operation["kind"] == "read_state.set" for operation in operations)


def test_committed_live_message_uses_read_state_without_replaying_message():
    store = FakeStore()
    event = {
        "id": 10,
        "type": "message",
        "flags": ["read"],
        "message": _stream_message(),
    }
    first = _operations(converter.event_records(store, ACCOUNT_UUID, "queue", event))
    created = next(
        operation for operation in first if operation["kind"] == "message.create"
    )
    mapping = store.provider_mapping(ACCOUNT_UUID, "message", "601")
    mapping["metadata"]["workspace_delivery_state"] = "committed"
    event["id"] = 11

    operations = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    )

    read = next(
        operation for operation in operations if operation["kind"] == "read_state.set"
    )
    assert not any(operation["kind"] == "message.create" for operation in operations)
    assert read["payload"] == {
        "stream_uuid": created["payload"]["stream_uuid"],
        "topic_uuid": created["payload"]["topic_uuid"],
        "reader_uuid": OWNER_UUID,
        "message_uuids": [created["entity_uuid"]],
        "read": True,
    }


def test_channel_message_does_not_overwrite_backend_owned_stream_projection():
    store = FakeStore()
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    participant_uuids = [
        OWNER_UUID,
        converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "2"),
        str(uuid.uuid4()),
    ]
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": participant_uuids,
            "name": "Canonical name",
            "description": "Canonical description",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:Topic",
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
    )
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 30, "type": "message", "message": _stream_message()},
    )
    operations = _operations(records)
    assert any(operation["kind"] == "message.create" for operation in operations)
    assert any(operation["kind"] == "topic.upsert" for operation in operations)
    assert not any(operation["kind"] == "stream.upsert" for operation in operations)
    metadata = store.provider_mapping(ACCOUNT_UUID, "stream", "channel:42")["metadata"]
    assert metadata["private"] is False
    assert metadata["name"] == "Canonical name"
    assert metadata["description"] == "Canonical description"
    assert set(metadata["participants"]).issuperset(participant_uuids)


def test_message_create_and_update_mentions_use_provider_identity_ids_and_urns():
    store = FakeStore(auto_materialize=False)
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "identity",
        "1",
        OWNER_UUID,
        {"display_name": "Owner", "active": True},
    )
    stream_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:Topic",
        str(uuid.uuid4()),
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
    )
    message = _stream_message()
    message["content"] = "hello @**Mentioned User|3** and @**Owner|1**"
    created = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 20, "type": "message", "message": message},
        )
    )
    mentioned_uuid = converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "3")
    identity = next(
        operation
        for operation in created
        if operation["kind"] == "identity.upsert"
        and operation["provider"]["entity_id"] == "3"
    )
    created_message = next(
        operation for operation in created if operation["kind"] == "message.create"
    )
    assert identity["entity_uuid"] == mentioned_uuid
    assert (
        f"[Mentioned User](urn:user:{mentioned_uuid})"
        in created_message["payload"]["payload"]["content"]
    )
    assert (
        f"[Owner](urn:user:{OWNER_UUID})"
        in created_message["payload"]["payload"]["content"]
    )
    assert not any(
        operation["kind"] == "identity.upsert"
        and operation["entity_uuid"] == OWNER_UUID
        for operation in created
    )

    resolved = []
    updated = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 21,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 42,
                "content": (
                    "edited @_**Another User|4** "
                    "[report.pdf](/user_uploads/a/report.pdf)"
                ),
                "edit_timestamp": 1_700_000_010,
            },
            file_resolver=lambda url, name: (
                resolved.append((url, name))
                or "urn:file:00000000-0000-0000-0000-000000000001"
            ),
        )
    )
    update_identity = next(
        operation for operation in updated if operation["kind"] == "identity.upsert"
    )
    updated_message = next(
        operation for operation in updated if operation["kind"] == "message.update"
    )
    assert not any(operation["kind"] == "topic.upsert" for operation in updated)
    assert updated_message["extensions"]["subject"] == "Topic"
    assert update_identity["provider"]["entity_id"] == "4"
    assert (
        "[Another User](urn:user:" in updated_message["payload"]["payload"]["content"]
    )
    assert "urn:file:" in updated_message["payload"]["payload"]["content"]
    assert updated_message["actor_uuid"] == created_message["payload"]["author_uuid"]
    assert resolved == [("/user_uploads/a/report.pdf", "report.pdf")]


def test_inbound_zulip_reply_resolves_provider_target_to_workspace_message():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message(601)},
    )
    original = store.provider_mapping(ACCOUNT_UUID, "message", "601")
    reply = _stream_message(602)
    reply["content"] = (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/601):\n"
        "```quote\noriginal\n```\n\nreply"
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": reply},
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["payload"]["reply_to_message_uuid"] == original["workspace_uuid"]
    assert message["payload"]["payload"]["content"] == (
        f"[Other User](urn:quote:{original['workspace_uuid']})\n\nreply"
    )
    assert message["extensions"]["unresolved_reply_provider_id"] is None


def test_inbound_zulip_reply_accepts_localized_quote_link_label():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message(601)},
    )
    original = store.provider_mapping(ACCOUNT_UUID, "message", "601")
    reply = _stream_message(602)
    reply["content"] = (
        "@_**Other User|2** "
        "[сказал/а](https://zulip.example.invalid/#narrow/near/601):\n"
        "```quote\noriginal\n```\n\nreply"
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": reply},
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["payload"]["reply_to_message_uuid"] == original["workspace_uuid"]
    assert message["payload"]["payload"]["content"] == (
        f"[Other User](urn:quote:{original['workspace_uuid']})\n\nreply"
    )
    assert message["extensions"]["unresolved_reply_provider_id"] is None


def test_inbound_zulip_reply_resolves_within_each_account_projection():
    second_account_uuid = str(uuid.uuid4())
    stores = (
        FakeStore(),
        FakeStore(
            account_uuid=second_account_uuid,
            owner_uuid=str(uuid.uuid4()),
            project_uuid=str(uuid.uuid4()),
        ),
    )
    account_uuids = (ACCOUNT_UUID, second_account_uuid)
    source_uuids = []

    for store, account_uuid in zip(stores, account_uuids, strict=True):
        converter.event_records(
            store,
            account_uuid,
            "queue",
            {"id": 10, "type": "message", "message": _stream_message(601)},
        )
        source = store.provider_mapping(account_uuid, "message", "601")
        source_uuids.append(source["workspace_uuid"])
        reply = _stream_message(602)
        reply["content"] = (
            "@_**Other User|2** "
            "[said](https://zulip.example.invalid/#narrow/near/601):\n"
            "```quote\noriginal\n```\n\nreply"
        )

        records = converter.event_records(
            store,
            account_uuid,
            "queue",
            {"id": 11, "type": "message", "message": reply},
        )
        message = next(
            operation
            for operation in _operations(records)
            if operation["kind"] == "message.create"
        )

        assert message["payload"]["reply_to_message_uuid"] == source["workspace_uuid"]
        assert message["payload"]["payload"]["content"] == (
            f"[Other User](urn:quote:{source['workspace_uuid']})\n\nreply"
        )
        assert message["extensions"]["unresolved_reply_provider_id"] is None

    assert source_uuids[0] != source_uuids[1]


@pytest.mark.parametrize(
    "content",
    (
        "[message](https://zulip.example.invalid/#narrow/near/601)",
        (
            "[said](https://zulip.example.invalid/#narrow/near/601):\n"
            "plain text, not a semantic quote"
        ),
        (
            "````markdown\n"
            "[said](https://zulip.example.invalid/#narrow/near/601):\n"
            "```quote\nliteral example\n```\n"
            "````"
        ),
    ),
)
def test_inbound_zulip_non_quote_links_do_not_create_reply_relationship(content):
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message(601)},
    )
    message = _stream_message(602)
    message["content"] = content

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": message},
    )
    created = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert created["payload"]["reply_to_message_uuid"] is None
    assert created["extensions"]["unresolved_reply_provider_id"] is None


def test_convergent_workspace_alias_replays_idempotent_backfill_upsert():
    store = FakeStore()
    workspace_uuid = str(uuid.uuid4())
    store.mappings[("message", "601")] = {
        "workspace_uuid": workspace_uuid,
        "provider_id": "601",
        "provider_revision": None,
        "metadata": {
            "mapping_origin": "workspace",
            "workspace_delivery_state": "committed",
        },
        "convergent_alias": True,
    }

    provider_message = _stream_message(601)
    provider_message["flags"] = ["read"]
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "history",
        {"id": 601, "type": "message", "message": provider_message},
        "backfill",
    )

    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.update"
    )
    assert message["entity_uuid"] == workspace_uuid
    assert message["payload"]["read"] is True
    assert not any(
        operation["kind"] == "read_state.set" for operation in _operations(records)
    )
    assert (
        store.provider_mapping(ACCOUNT_UUID, "message", "601")["workspace_uuid"]
        == workspace_uuid
    )


def test_provider_mapping_before_event_delivery_replays_same_workspace_message_uuid():
    store = FakeStore()
    pending_workspace_uuid = str(uuid.uuid4())
    store.mappings[("message", "601")] = {
        "workspace_uuid": pending_workspace_uuid,
        "provider_id": "601",
        "provider_revision": None,
        "metadata": {
            "mapping_origin": "zulip",
            "workspace_delivery_state": "pending",
        },
        "convergent_alias": False,
    }

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "history",
        {"id": 601, "type": "message", "message": _stream_message(601)},
        "backfill",
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["entity_uuid"] == pending_workspace_uuid
    assert message["entity_uuid"] != converter.stable_entity_uuid(
        ACCOUNT_UUID, "message", "601"
    )


def test_live_message_replay_keeps_digest_after_topic_recanonicalization():
    class InitiallyUnmappedMentionStore(FakeStore):
        def provider_mapping(self, account_uuid, entity_kind, provider_id):
            if (
                entity_kind == "identity"
                and provider_id == "99"
                and (entity_kind, provider_id) not in self.mappings
            ):
                return None
            return super().provider_mapping(account_uuid, entity_kind, provider_id)

    store = InitiallyUnmappedMentionStore()
    event = {
        "id": 17,
        "type": "message",
        "message": _stream_message(601, "✔ resolved topic"),
    }
    event["message"]["content"] = (
        "#**Engineering>✔ resolved topic** @**Mentioned user|99**"
    )
    accepted = converter.event_records(store, ACCOUNT_UUID, "queue", event)
    assert any(
        record["operation"]["kind"] == "identity.upsert"
        and record["operation"]["provider"]["entity_id"] == "99"
        for record in accepted
    )
    message_mapping = store.mappings[("message", "601")]
    original_topic_uuid = message_mapping["metadata"]["topic_uuid"]
    accepted_message = next(
        record for record in accepted if record["operation"]["kind"] == "message.create"
    )
    accepted_payload = accepted_message["operation"]["payload"]
    store.accepted_contexts[(ACCOUNT_UUID, "queue", 17)] = {
        "project_uuid": accepted_message["project_uuid"],
        "message_uuid": accepted_message["operation"]["entity_uuid"],
        "chat_key": accepted_message["operation"]["provider"]["chat_id"],
        "stream_uuid": accepted_payload["stream_uuid"],
        "topic_uuid": accepted_payload["topic_uuid"],
        "author_uuid": accepted_payload["author_uuid"],
        "message_operation": accepted_message["operation"],
        "accepted_records": accepted,
        "accepted_records_complete": True,
    }
    message_mapping["metadata"]["workspace_delivery_state"] = "committed"
    del message_mapping["metadata"]["provider_event_id"]
    topic_mapping = store.mappings[("topic", "42:✔ resolved topic")]
    canonical_topic_uuid = str(uuid.uuid4())
    topic_mapping["workspace_uuid"] = canonical_topic_uuid
    message_mapping["metadata"]["topic_uuid"] = canonical_topic_uuid

    replay = converter.event_records(store, ACCOUNT_UUID, "queue", event)

    assert [
        (
            record["operation_uuid"],
            record["operation"]["kind"],
            record["operation_sha256"],
        )
        for record in replay
    ] == [
        (
            record["operation_uuid"],
            record["operation"]["kind"],
            record["operation_sha256"],
        )
        for record in accepted
    ]
    replay_message = next(
        operation
        for operation in _operations(replay)
        if operation["kind"] == "message.create"
    )
    assert replay_message["payload"]["topic_uuid"] == original_topic_uuid
    assert (
        replay_message["payload"]["payload"]["content"]
        == accepted_payload["payload"]["content"]
    )
    assert f"urn:topic:{original_topic_uuid}" in accepted_payload["payload"]["content"]


def test_live_message_replay_rejects_partial_accepted_sequence():
    store = FakeStore()
    event = {
        "id": 17,
        "type": "message",
        "message": _stream_message(601, "Topic"),
    }
    event["message"]["flags"] = ["read"]
    accepted = converter.event_records(store, ACCOUNT_UUID, "queue", event)
    assert accepted[-1]["operation"]["kind"] == "message.create"
    accepted_message = next(
        record for record in accepted if record["operation"]["kind"] == "message.create"
    )
    accepted_payload = accepted_message["operation"]["payload"]
    store.accepted_contexts[(ACCOUNT_UUID, "queue", 17)] = {
        "project_uuid": accepted_message["project_uuid"],
        "message_uuid": accepted_message["operation"]["entity_uuid"],
        "chat_key": accepted_message["operation"]["provider"]["chat_id"],
        "stream_uuid": accepted_payload["stream_uuid"],
        "topic_uuid": accepted_payload["topic_uuid"],
        "author_uuid": accepted_payload["author_uuid"],
        "message_operation": accepted_message["operation"],
        "accepted_records": accepted[:-1],
        "accepted_records_complete": False,
    }

    with pytest.raises(ValueError, match="provider_event_replay_incomplete"):
        converter.event_records(store, ACCOUNT_UUID, "queue", event)


def test_live_message_replay_accepts_legacy_unscoped_operation_ids():
    event = {
        "id": 17,
        "type": "message",
        "message": _stream_message(601, "Topic"),
    }
    accepted = converter.event_records(
        FakeStore(),
        ACCOUNT_UUID,
        "queue",
        event,
    )
    legacy = copy.deepcopy(accepted)
    operation_uuids = {
        record["operation_uuid"]: converter.operation_uuid_for(
            ACCOUNT_UUID,
            "provider-message:601",
            17,
            index,
        )
        for index, record in enumerate(legacy)
    }
    for record in legacy:
        operation_uuid = operation_uuids[record["operation_uuid"]]
        predecessor = record["predecessor_operation_uuid"]
        record["operation_uuid"] = operation_uuid
        record["record_uuid"] = str(
            uuid.uuid5(
                converter.OPERATION_NAMESPACE,
                f"{operation_uuid}:record",
            )
        )
        record["predecessor_operation_uuid"] = operation_uuids.get(
            predecessor,
            predecessor,
        )
        record["operation_sha256"] = canonical.operation_digest(record)

    assert (
        converter._accepted_live_replay_records(
            ACCOUNT_UUID,
            event,
            {
                "project_uuid": PROJECT_UUID,
                "accepted_records": legacy,
                "accepted_records_complete": True,
            },
        )
        == legacy
    )


def test_different_live_event_does_not_recreate_committed_provider_message():
    store = FakeStore()
    event = {"id": 17, "type": "message", "message": _stream_message(601)}
    converter.event_records(store, ACCOUNT_UUID, "queue", event)
    store.mappings[("message", "601")]["metadata"]["workspace_delivery_state"] = (
        "committed"
    )

    repeated_event = {**event, "id": 18}
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        repeated_event,
    )

    assert not any(
        operation["kind"] == "message.create" for operation in _operations(records)
    )


def test_unresolved_inbound_zulip_reply_is_preserved_as_safe_fallback():
    store = FakeStore()
    reply = _stream_message(602)
    reply["content"] = "[said](#narrow/near/999):\n```quote\nmissing\n```\n\nreply"

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": reply},
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["payload"]["reply_to_message_uuid"] is None
    assert message["extensions"]["unresolved_reply_provider_id"] == "999"
    assert "missing" in message["payload"]["payload"]["content"]


def test_subscription_rename_reuses_stream_uuid():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )
    mapping = store.provider_mapping(ACCOUNT_UUID, "stream", "channel:42")
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 14,
            "type": "subscription",
            "op": "update",
            "property": "name",
            "stream_id": 42,
            "value": "Platform",
        },
    )
    operation = records[0]["operation"]
    assert operation["kind"] == "stream.upsert"
    assert operation["entity_uuid"] == mapping["workspace_uuid"]
    assert operation["payload"]["name"] == "Platform"


def test_subscription_notification_updates_converge_and_drop_stale_events():
    store = FakeStore()
    snapshot = {
        "id": -1,
        "type": "subscription",
        "op": "notification_snapshot",
        "stream_id": 42,
        "is_muted": False,
        "desktop_notifications": False,
        "enable_stream_desktop_notifications": True,
        "observed_at": 1_800_000_000,
    }

    snapshot_operation = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", snapshot)
    )[0]
    assert snapshot_operation["kind"] == "stream.notification.update"
    assert snapshot_operation["payload"]["notification_mode"] == "mentions_only"

    mute_operation = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "subscription",
                "op": "update",
                "property": "is_muted",
                "stream_id": 42,
                "value": True,
                "observed_at": 1_800_000_010,
            },
        )
    )[0]
    assert mute_operation["payload"]["notification_mode"] == "muted"

    stale = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {**snapshot, "id": -2, "desktop_notifications": True},
    )
    assert stale == []
    metadata = store.provider_mapping(ACCOUNT_UUID, "stream", "channel:42")["metadata"]
    assert metadata["notification_mode"] == "muted"
    assert metadata["notification_updated_at"] == "2027-01-15T08:00:10Z"


def test_inherited_stream_notification_mode_tracks_global_setting():
    store = FakeStore()
    snapshot = {
        "id": -1,
        "type": "subscription",
        "op": "notification_snapshot",
        "stream_id": 42,
        "is_muted": False,
        "desktop_notifications": None,
        "enable_stream_desktop_notifications": True,
        "observed_at": 1_800_000_000,
    }

    initial = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", snapshot)
    )
    assert initial[0]["payload"]["notification_mode"] == "all_messages"

    changed = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 11,
                "type": "user_settings",
                "op": "update",
                "property": "enable_stream_desktop_notifications",
                "value": False,
                "subscriptions": [
                    {
                        "stream_id": 42,
                        "is_muted": False,
                        "desktop_notifications": None,
                    }
                ],
                "observed_at": 1_800_000_010,
            },
        )
    )
    assert changed[0]["payload"]["notification_mode"] == "mentions_only"


@pytest.mark.parametrize(
    ("is_muted", "desktop_notifications", "expected_mode"),
    [(False, True, "all_messages"), (True, None, "muted")],
)
def test_global_notification_update_preserves_unaffected_stream_mode(
    is_muted,
    desktop_notifications,
    expected_mode,
):
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": -1,
            "type": "subscription",
            "op": "notification_snapshot",
            "stream_id": 42,
            "is_muted": is_muted,
            "desktop_notifications": desktop_notifications,
            "enable_stream_desktop_notifications": True,
            "observed_at": 1_800_000_000,
        },
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 11,
            "type": "user_settings",
            "op": "update",
            "property": "enable_stream_desktop_notifications",
            "value": False,
            "subscriptions": [
                {
                    "stream_id": 42,
                    "is_muted": is_muted,
                    "desktop_notifications": desktop_notifications,
                }
            ],
            "observed_at": 1_800_000_010,
        },
    )

    assert records == []
    metadata = store.provider_mapping(ACCOUNT_UUID, "stream", "channel:42")["metadata"]
    assert metadata["notification_mode"] == expected_mode
    assert metadata["notification_global_desktop_notifications"] is False
    assert metadata["notification_updated_at"] == "2027-01-15T08:00:00Z"
    assert metadata["notification_global_updated_at"] == "2027-01-15T08:00:10Z"


@pytest.mark.parametrize(
    ("visibility_policy", "expected_mode"),
    [(0, "default"), (1, "mute"), (2, "unmute"), (3, "follow")],
)
def test_user_topic_visibility_policy_maps_to_workspace_mode(
    visibility_policy,
    expected_mode,
):
    store = FakeStore()
    event = {
        "id": 12,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "bridge",
        "visibility_policy": visibility_policy,
        "last_updated": 1_800_000_020,
    }

    operations = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    )

    assert len(operations) == 1
    assert operations[0]["kind"] == "topic.notification.update"
    assert operations[0]["payload"]["notification_mode"] == expected_mode
    assert operations[0]["payload"]["notification_updated_at"] == (
        "2027-01-15T08:00:20Z"
    )
    metadata = store.provider_mapping(
        ACCOUNT_UUID, "topic", "42:bridge"
    )["metadata"]
    assert metadata["notification_provider_updated_at"] == 1_800_000_020
    assert (
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {**event, "id": 13},
        )
        == []
    )


def test_user_topic_same_second_changes_advance_logical_timestamp():
    store = FakeStore()
    first = {
        "id": 12,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "bridge",
        "visibility_policy": 1,
        "last_updated": 1_800_000_020,
    }

    first_operation = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", first)
    )[0]
    second_operation = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {**first, "id": 13, "visibility_policy": 3},
        )
    )[0]

    assert first_operation["payload"]["notification_mode"] == "mute"
    assert first_operation["payload"]["notification_updated_at"] == (
        "2027-01-15T08:00:20Z"
    )
    assert second_operation["payload"]["notification_mode"] == "follow"
    assert second_operation["payload"]["notification_updated_at"] == (
        "2027-01-15T08:00:20.000001Z"
    )
    assert (
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {**first, "id": 14, "visibility_policy": 3},
        )
        == []
    )


def test_user_topic_for_retired_catalog_chat_does_not_wait_for_assignment():
    store = FakeStore(auto_materialize=False)
    store.assignment_for_provider_chat = lambda *_args: None
    store.provider_chat_is_cataloged = lambda *_args: False
    event = {
        "id": -1,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "retired",
        "visibility_policy": 1,
        "last_updated": 1_800_000_020,
    }

    with pytest.raises(ValueError, match="provider_chat_not_selected"):
        converter.event_records(store, ACCOUNT_UUID, "queue", event)


def test_user_topic_for_cataloged_chat_still_waits_for_assignment():
    store = FakeStore(auto_materialize=False)
    store.assignment_for_provider_chat = lambda *_args: None
    store.provider_chat_is_cataloged = lambda *_args: True
    event = {
        "id": -1,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "pending",
        "visibility_policy": 1,
        "last_updated": 1_800_000_020,
    }

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(store, ACCOUNT_UUID, "queue", event)


def test_live_user_topic_for_missing_catalog_still_waits_for_assignment():
    store = FakeStore(auto_materialize=False)
    store.assignment_for_provider_chat = lambda *_args: None
    store.provider_chat_is_cataloged = lambda *_args: False
    event = {
        "id": 1,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "new",
        "visibility_policy": 1,
        "last_updated": 1_800_000_020,
    }

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(store, ACCOUNT_UUID, "queue", event)


def test_registration_default_tombstone_clears_missing_user_topic_override():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue-old",
        {
            "id": 12,
            "type": "user_topic",
            "stream_id": 42,
            "topic_name": "bridge",
            "visibility_policy": 1,
            "last_updated": 1_800_000_010,
        },
    )

    operation = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue-new",
            {
                "id": -1,
                "type": "user_topic",
                "stream_id": 42,
                "topic_name": "bridge",
                "visibility_policy": 0,
                "observed_at": 1_800_000_020.5,
            },
        )
    )[0]

    assert operation["payload"]["notification_mode"] == "default"
    assert operation["payload"]["notification_updated_at"] == (
        "2027-01-15T08:00:20.500000Z"
    )


def test_newest_first_uses_timestamp_then_numeric_message_id():
    messages = [
        {"id": 1, "timestamp": 10},
        {"id": 3, "timestamp": 10},
        {"id": 2, "timestamp": 11},
    ]
    assert [message["id"] for message in converter.newest_first(messages)] == [
        2,
        3,
        1,
    ]
