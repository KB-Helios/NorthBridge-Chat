use northbridge_domain::{
    AgentRunStatus, AiProvider, ArtifactKind, CrewMember, CrewRunStatus, DEFAULT_WORKSPACE_ID,
    DESKTOP_SETTINGS_SCOPE, GenerationStatus, KnowledgeSourceKind, MODEL_SELECTION_SETTING_KEY,
    ManagedModel, ManagedModelSource, ManagedModelStatus, MessagePartKind, MessageRole,
    ModelLoadProfile, ModelQuantization, ModelSelection, NewMessage, NewMessagePart,
    ShadowSuggestionStatus, TeamRole, ToolCallStatus, ToolPolicyMode, TransformOperation,
    WorkflowNode, WorkflowNodeKind, WorkflowNodeRunStatus, WorkflowRunStatus,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    Database, NewAgentProfile, NewAgentRun, NewArtifact, NewCrew, NewCrewRun, NewKnowledgeChunk,
    NewKnowledgeDocument, NewKnowledgeStack, NewRemoteAgent, NewShadowSubscription, NewSyncChange,
    NewTeamPolicy, NewTeamUser, NewToolIntent, NewWorkflow, NewWorkflowRun, StorageError,
};

async fn test_database() -> (Database, TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::connect(&directory.path().join("storage.sqlite"))
        .await
        .unwrap();
    (database, directory)
}

#[tokio::test]
async fn persists_workflow_attempts_and_never_retries_ambiguous_recovery() {
    let (database, _directory) = test_database().await;
    let workflow = database
        .create_workflow(NewWorkflow {
            workspace_id: workspace_id(),
            name: "Recovery workflow".to_string(),
            description: String::new(),
            entry_node_id: "safe".to_string(),
            nodes: vec![
                WorkflowNode {
                    id: "safe".to_string(),
                    kind: WorkflowNodeKind::Transform {
                        operation: TransformOperation::Identity,
                    },
                    next: Some("effect".to_string()),
                },
                WorkflowNode {
                    id: "effect".to_string(),
                    kind: WorkflowNodeKind::Tool {
                        agent_id: Uuid::now_v7(),
                        instruction: "mutate external state".to_string(),
                    },
                    next: None,
                },
            ],
        })
        .await
        .unwrap();
    let run = database
        .create_workflow_run(NewWorkflowRun {
            workflow_id: workflow.id,
            workspace_id: workspace_id(),
            parent_run_id: None,
            input: json!({"value": 1}),
        })
        .await
        .unwrap();
    database
        .set_workflow_run_state(
            run.id,
            WorkflowRunStatus::Running,
            Some("effect"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let safe = database
        .create_workflow_node_run(run.id, "root/safe", "transform", &run.input, None, None)
        .await
        .unwrap();
    let effect = database
        .create_workflow_node_run(
            run.id,
            "root/effect",
            "tool",
            &run.input,
            None,
            Some("workflow-side-effect-key"),
        )
        .await
        .unwrap();

    assert_eq!(database.recover_interrupted_workflows().await.unwrap(), 2);
    assert_eq!(
        database.workflow_node_run(safe.id).await.unwrap().status,
        WorkflowNodeRunStatus::Failed
    );
    assert_eq!(
        database.workflow_node_run(effect.id).await.unwrap().status,
        WorkflowNodeRunStatus::UnknownOutcome
    );
    let recovered = database.workflow_run(run.id).await.unwrap();
    assert_eq!(recovered.status, WorkflowRunStatus::UnknownOutcome);
    assert!(
        recovered
            .last_error
            .unwrap()
            .contains("will not be retried")
    );
}

#[tokio::test]
async fn deduplicates_and_verifies_content_addressed_artifacts() {
    let (database, _directory) = test_database().await;
    let create = |name: &str, content: &[u8]| NewArtifact {
        workspace_id: workspace_id(),
        name: name.to_string(),
        kind: ArtifactKind::Document,
        media_type: "text/plain".to_string(),
        source: "test".to_string(),
        metadata: json!({}),
        content: content.to_vec(),
    };
    let first = database
        .create_artifact(create("first.txt", b"shared"))
        .await
        .unwrap();
    let second = database
        .create_artifact(create("second.txt", b"shared"))
        .await
        .unwrap();
    assert_eq!(first.sha256, second.sha256);
    let blobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(blobs, 1);

    let revised = database
        .revise_artifact(first.id, create("first.txt", b"revised"))
        .await
        .unwrap();
    assert_eq!(revised.revision, 2);
    assert_ne!(revised.sha256, first.sha256);
    database.delete_artifact(second.id).await.unwrap();
    let blobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(blobs, 1);
    let (_, content) = database.artifact_content(first.id).await.unwrap();
    assert_eq!(content, b"revised");
}

#[tokio::test]
async fn enforces_team_policy_audit_chain_and_idempotent_sync_conflicts() {
    let (database, _directory) = test_database().await;
    let user = database
        .create_team_user(NewTeamUser {
            workspace_id: workspace_id(),
            email: "owner@example.com".to_string(),
            display_name: "Owner".to_string(),
            password_hash: "x".repeat(64),
            role: TeamRole::Owner,
        })
        .await
        .unwrap();
    database
        .create_auth_session(user.id, &"a".repeat(64), crate::now_ms() + 60_000)
        .await
        .unwrap();
    assert_eq!(
        database
            .authenticate_session_hash(&"a".repeat(64))
            .await
            .unwrap()
            .unwrap()
            .id,
        user.id
    );
    database
        .create_team_policy(NewTeamPolicy {
            workspace_id: workspace_id(),
            name: "Deny member writes".to_string(),
            method: "POST".to_string(),
            path_prefix: "/v1/artifacts".to_string(),
            minimum_role: TeamRole::Owner,
            effect: "allow".to_string(),
        })
        .await
        .unwrap();
    assert!(
        !database
            .team_policy_allows(workspace_id(), TeamRole::Member, "POST", "/v1/artifacts")
            .await
            .unwrap()
    );
    database
        .append_audit_event(
            workspace_id(),
            "user:test",
            "POST",
            "/v1/artifacts",
            "201",
            Some("request-1"),
            &json!({"kind":"document"}),
        )
        .await
        .unwrap();
    database
        .append_audit_event(
            workspace_id(),
            "user:test",
            "GET",
            "/v1/artifacts",
            "200",
            Some("request-2"),
            &json!({}),
        )
        .await
        .unwrap();
    assert!(database.verify_audit_chain().await.unwrap());

    let device_id = Uuid::now_v7();
    let change = NewSyncChange {
        workspace_id: workspace_id(),
        entity_type: "setting".to_string(),
        entity_id: "theme".to_string(),
        base_version: 0,
        deleted: false,
        payload: json!({"value":"dark"}),
        device_id,
        client_change_id: "change-1".to_string(),
    };
    let first = database.apply_sync_change(change.clone()).await.unwrap();
    let retry = database.apply_sync_change(change).await.unwrap();
    assert_eq!(retry.sequence, first.sequence);
    let conflict = database
        .apply_sync_change(NewSyncChange {
            workspace_id: workspace_id(),
            entity_type: "setting".to_string(),
            entity_id: "theme".to_string(),
            base_version: 0,
            deleted: false,
            payload: json!({"value":"light"}),
            device_id,
            client_change_id: "change-2".to_string(),
        })
        .await;
    assert!(matches!(conflict, Err(StorageError::Conflict(_))));
}

fn workspace_id() -> Uuid {
    Uuid::parse_str(DEFAULT_WORKSPACE_ID).unwrap()
}

fn text_part(content: &str) -> NewMessagePart {
    NewMessagePart {
        kind: MessagePartKind::Text,
        content: content.to_string(),
        metadata: None,
    }
}

fn test_hash(character: char) -> String {
    character.to_string().repeat(64)
}

#[tokio::test]
async fn configures_wal_foreign_keys_and_embedded_migrations() {
    let (database, _directory) = test_database().await;
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(database.pool())
        .await
        .unwrap();
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(journal_mode, "wal");
    assert_eq!(foreign_keys, 1);
    assert_eq!(database.schema_version().await.unwrap(), 11);
}

#[tokio::test]
async fn persists_agent_checkpoints_and_tool_intent_before_side_effects() {
    let (database, _directory) = test_database().await;
    let agent = database
        .create_agent_profile(NewAgentProfile {
            workspace_id: workspace_id(),
            persona_id: None,
            name: "Safety agent".to_string(),
            description: String::new(),
            model_alias: "fast-local".to_string(),
            instructions: String::new(),
            policy_mode: ToolPolicyMode::SideEffects,
            max_turns: 8,
            skill_ids: Vec::new(),
            knowledge_stack_ids: Vec::new(),
            tool_allowlist: Vec::new(),
        })
        .await
        .unwrap();
    let run = database
        .create_agent_run(NewAgentRun {
            workspace_id: workspace_id(),
            agent_id: agent.id,
            conversation_id: None,
            prompt: "Change external state".to_string(),
            rig_version: "rig-agent/0.41.0+northbridge/1".to_string(),
            checkpoint: json!({"state":"executing_tools"}),
        })
        .await
        .unwrap();
    let (call, approval) = database
        .record_tool_intent(NewToolIntent {
            run_id: run.id,
            server_id: None,
            rig_tool_call_id: "call-1".to_string(),
            tool_name: "write_file".to_string(),
            arguments: json!({"path":"report.txt"}),
            read_only: false,
            destructive: true,
            requires_approval: true,
            approval_reason: "writes a file".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(call.status, ToolCallStatus::PendingApproval);
    let approval = approval.expect("approval persisted with intent");
    database
        .decide_approval(approval.id, true, Some("approved in test"))
        .await
        .unwrap();
    let claimed = database.claim_tool_call(call.id).await.unwrap();
    assert_eq!(claimed.status, ToolCallStatus::Running);

    assert_eq!(database.recover_interrupted_agent_work().await.unwrap(), 1);
    assert_eq!(
        database.agent_tool_call(call.id).await.unwrap().status,
        ToolCallStatus::UnknownOutcome
    );
    assert_eq!(
        database.agent_run(run.id).await.unwrap().status,
        AgentRunStatus::Queued
    );
}

#[tokio::test]
async fn shadow_suggestions_reserve_before_inference_and_resume_idempotently() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Shadowed")
        .await
        .unwrap();
    let message = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("Help me make this plan safer")],
        })
        .await
        .unwrap();
    let agent = database
        .create_agent_profile(NewAgentProfile {
            workspace_id: workspace_id(),
            persona_id: None,
            name: "Shadow coach".to_string(),
            description: String::new(),
            model_alias: "fast-local".to_string(),
            instructions: "Offer one concise suggestion.".to_string(),
            policy_mode: ToolPolicyMode::Manual,
            max_turns: 4,
            skill_ids: Vec::new(),
            knowledge_stack_ids: Vec::new(),
            tool_allowlist: Vec::new(),
        })
        .await
        .unwrap();
    let subscription = database
        .create_shadow_subscription(NewShadowSubscription {
            workspace_id: workspace_id(),
            conversation_id: conversation.id,
            agent_id: agent.id,
        })
        .await
        .unwrap();
    let reserved = database
        .reserve_shadow_suggestion(subscription.id, message.id)
        .await
        .unwrap();
    let duplicate = database
        .reserve_shadow_suggestion(subscription.id, message.id)
        .await
        .unwrap();
    assert_eq!(duplicate.id, reserved.id);
    assert_eq!(reserved.status, ShadowSuggestionStatus::Processing);
    assert_eq!(
        database
            .shadow_subscription(subscription.id)
            .await
            .unwrap()
            .last_message_id,
        Some(message.id)
    );

    let run = database
        .create_agent_run(NewAgentRun {
            workspace_id: workspace_id(),
            agent_id: agent.id,
            conversation_id: Some(conversation.id),
            prompt: "Offer a suggestion".to_string(),
            rig_version: "rig-agent/0.41.0+northbridge/1".to_string(),
            checkpoint: json!({"state":"queued"}),
        })
        .await
        .unwrap();
    database
        .attach_shadow_suggestion_run(reserved.id, run.id)
        .await
        .unwrap();
    assert_eq!(
        database
            .list_processing_shadow_suggestions(workspace_id())
            .await
            .unwrap()
            .len(),
        1
    );
    let completed = database
        .complete_shadow_suggestion(reserved.id, "Add a rollback step.")
        .await
        .unwrap();
    assert_eq!(completed.status, ShadowSuggestionStatus::Unread);
    assert_eq!(
        database
            .decide_shadow_suggestion(reserved.id, true)
            .await
            .unwrap()
            .status,
        ShadowSuggestionStatus::Accepted
    );
}

#[tokio::test]
async fn crew_runs_persist_local_members_and_do_not_retry_interrupted_remote_calls() {
    let (database, _directory) = test_database().await;
    let coordinator = database
        .create_agent_profile(NewAgentProfile {
            workspace_id: workspace_id(),
            persona_id: None,
            name: "Coordinator".to_string(),
            description: String::new(),
            model_alias: "fast-local".to_string(),
            instructions: String::new(),
            policy_mode: ToolPolicyMode::SideEffects,
            max_turns: 8,
            skill_ids: Vec::new(),
            knowledge_stack_ids: Vec::new(),
            tool_allowlist: Vec::new(),
        })
        .await
        .unwrap();
    let specialist = database
        .create_agent_profile(NewAgentProfile {
            workspace_id: workspace_id(),
            persona_id: None,
            name: "Specialist".to_string(),
            description: String::new(),
            model_alias: "fast-local".to_string(),
            instructions: String::new(),
            policy_mode: ToolPolicyMode::SideEffects,
            max_turns: 8,
            skill_ids: Vec::new(),
            knowledge_stack_ids: Vec::new(),
            tool_allowlist: Vec::new(),
        })
        .await
        .unwrap();
    let remote = database
        .upsert_remote_agent(NewRemoteAgent {
            workspace_id: workspace_id(),
            name: "Remote reviewer".to_string(),
            upstream_url: "https://agents.example.test/reviewer".to_string(),
            auth_environment_key: None,
            enabled: true,
        })
        .await
        .unwrap();
    let crew = database
        .create_crew(NewCrew {
            workspace_id: workspace_id(),
            name: "Review crew".to_string(),
            description: String::new(),
            coordinator_agent_id: coordinator.id,
            members: vec![
                CrewMember {
                    ordinal: 0,
                    role: "Safety reviewer".to_string(),
                    local_agent_id: Some(specialist.id),
                    remote_agent_id: None,
                },
                CrewMember {
                    ordinal: 1,
                    role: "External reviewer".to_string(),
                    local_agent_id: None,
                    remote_agent_id: Some(remote.id),
                },
            ],
        })
        .await
        .unwrap();
    let run = database
        .create_crew_run(NewCrewRun {
            workspace_id: workspace_id(),
            crew_id: crew.id,
            prompt: "Review the rollout".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(run.status, CrewRunStatus::Queued);
    database.start_crew_run(run.id).await.unwrap();
    let member_run = database
        .create_agent_run(NewAgentRun {
            workspace_id: workspace_id(),
            agent_id: specialist.id,
            conversation_id: None,
            prompt: "Review".to_string(),
            rig_version: "rig-agent/0.41.0+northbridge/1".to_string(),
            checkpoint: json!({"state":"queued"}),
        })
        .await
        .unwrap();
    database
        .start_local_crew_member(run.id, 0, member_run.id)
        .await
        .unwrap();
    database
        .finish_crew_member(crate::FinishCrewMember {
            crew_run_id: run.id,
            ordinal: 0,
            output: Some("Looks safe"),
            response: None,
            remote_task_id: None,
            remote_context_id: None,
            error: None,
        })
        .await
        .unwrap();
    database.start_remote_crew_member(run.id, 1).await.unwrap();
    assert_eq!(
        database.crew_run(run.id).await.unwrap().members[0].status,
        "completed"
    );
    assert_eq!(database.recover_interrupted_crew_runs().await.unwrap(), 2);
    let recovered = database.crew_run(run.id).await.unwrap();
    assert_eq!(recovered.status, CrewRunStatus::Suspended);
    assert_eq!(recovered.members[1].status, "unknown_outcome");
}

#[tokio::test]
async fn persists_model_download_progress_and_recovers_interrupted_jobs() {
    let (database, directory) = test_database().await;
    let timestamp = 1_000;
    let model = ManagedModel {
        id: Uuid::now_v7(),
        model_id: "org/model".to_string(),
        display_name: "Model".to_string(),
        source: ManagedModelSource::HuggingFace,
        source_reference: "org/model".to_string(),
        revision: Some("main".to_string()),
        local_path: directory
            .path()
            .join("model")
            .to_string_lossy()
            .into_owned(),
        status: ManagedModelStatus::Downloading,
        bytes_downloaded: 0,
        total_bytes: None,
        file_count: None,
        manifest_sha256: None,
        last_error: None,
        created_at_ms: timestamp,
        updated_at_ms: timestamp,
    };
    database.upsert_managed_model(&model).await.unwrap();
    database
        .update_managed_model_progress(model.id, 128, Some(512), Some(4))
        .await
        .unwrap();
    let progressing = database.managed_model(model.id).await.unwrap();
    assert_eq!(progressing.bytes_downloaded, 128);
    assert_eq!(progressing.total_bytes, Some(512));
    assert_eq!(progressing.file_count, Some(4));

    assert_eq!(
        database
            .recover_interrupted_model_downloads()
            .await
            .unwrap(),
        1
    );
    let recovered = database.managed_model(model.id).await.unwrap();
    assert_eq!(recovered.status, ManagedModelStatus::Failed);
    assert!(recovered.last_error.unwrap().contains("interrupted"));
}

#[tokio::test]
async fn persists_model_selection_and_generation_provenance() {
    let (database, _directory) = test_database().await;
    let selection = ModelSelection {
        provider: AiProvider::LocalMistralRs,
        model_id: "HuggingFaceTB/SmolLM2-135M-Instruct".to_string(),
        model_alias: "fast-local".to_string(),
        load_profile: ModelLoadProfile {
            quantization: ModelQuantization::Q4K,
            ..ModelLoadProfile::default()
        },
    };
    let value = serde_json::to_value(&selection).unwrap();
    database
        .set_json_setting(DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY, &value)
        .await
        .unwrap();
    let stored = database
        .get_json_setting(DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_value::<ModelSelection>(stored).unwrap(),
        selection
    );

    let conversation = database
        .create_conversation(workspace_id(), "Model provenance")
        .await
        .unwrap();
    let run = database
        .create_generation_run_for_model_idempotent(
            conversation.id,
            conversation.default_branch_id,
            &selection,
            Some("model-provenance-1"),
        )
        .await
        .unwrap();
    assert_eq!(run.provider, Some(AiProvider::LocalMistralRs));
    assert_eq!(run.model_id.as_deref(), Some(selection.model_id.as_str()));
    assert_eq!(run.load_profile.as_ref(), Some(&selection.load_profile));

    assert!(
        database
            .delete_setting(DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY)
            .await
            .unwrap()
    );
    assert!(
        database
            .get_json_setting(DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn preserves_branch_heads_paginates_and_updates_fts() {
    let (database, _directory) = test_database().await;
    let first = database
        .create_conversation(workspace_id(), "First")
        .await
        .unwrap();
    let second = database
        .create_conversation(workspace_id(), "Second")
        .await
        .unwrap();
    let third = database
        .create_conversation(workspace_id(), "Third")
        .await
        .unwrap();
    let page_one = database
        .list_conversations(workspace_id(), None, 2)
        .await
        .unwrap();
    assert_eq!(page_one.items.len(), 2);
    let page_two = database
        .list_conversations(workspace_id(), page_one.next_cursor.as_ref(), 2)
        .await
        .unwrap();
    assert_eq!(page_two.items.len(), 1);
    let mut ids = page_one
        .items
        .iter()
        .chain(&page_two.items)
        .map(|conversation| conversation.id)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&first.id));
    assert!(ids.contains(&second.id));
    assert!(ids.contains(&third.id));

    let user = database
        .append_message(NewMessage {
            conversation_id: first.id,
            branch_id: first.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("SQLite hybrid retrieval")],
        })
        .await
        .unwrap();
    let stale_append = database
        .append_message(NewMessage {
            conversation_id: first.id,
            branch_id: first.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("stale writer")],
        })
        .await;
    assert!(matches!(stale_append, Err(StorageError::Conflict(_))));
    database
        .append_message(NewMessage {
            conversation_id: first.id,
            branch_id: first.default_branch_id,
            parent_message_id: Some(user.id),
            role: MessageRole::Assistant,
            parts: vec![text_part("Indexed response")],
        })
        .await
        .unwrap();
    let messages = database
        .list_messages(first.id, first.default_branch_id)
        .await
        .unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].parent_message_id, Some(user.id));

    let hits = database
        .search_messages(workspace_id(), "hybrid retrieval", 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].message_id, user.id);
    database.soft_delete_conversation(first.id).await.unwrap();
    let deleted_append = database
        .append_message(NewMessage {
            conversation_id: first.id,
            branch_id: first.default_branch_id,
            parent_message_id: Some(messages[1].id),
            role: MessageRole::User,
            parts: vec![text_part("must not be written")],
        })
        .await;
    assert!(matches!(
        deleted_append,
        Err(StorageError::NotFound { entity: "branch" })
    ));
    let hits = database
        .search_messages(workspace_id(), "hybrid retrieval", 10)
        .await
        .unwrap();
    assert!(hits.is_empty());
}

#[tokio::test]
async fn forks_immutable_message_paths_and_activates_the_new_branch() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Branches")
        .await
        .unwrap();
    let user = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("Give me an answer")],
        })
        .await
        .unwrap();
    let first_answer = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: Some(user.id),
            role: MessageRole::Assistant,
            parts: vec![text_part("First answer")],
        })
        .await
        .unwrap();

    let fork = database
        .fork_conversation_branch(conversation.id, user.id, true)
        .await
        .unwrap();
    let second_answer = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: fork.id,
            parent_message_id: Some(user.id),
            role: MessageRole::Assistant,
            parts: vec![text_part("Second answer")],
        })
        .await
        .unwrap();

    let active = database.get_conversation(conversation.id).await.unwrap();
    assert_eq!(active.default_branch_id, fork.id);
    let active_path = database
        .list_messages(conversation.id, fork.id)
        .await
        .unwrap();
    assert_eq!(
        active_path
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        [user.id, second_answer.id]
    );
    let original_path = database
        .list_messages(conversation.id, conversation.default_branch_id)
        .await
        .unwrap();
    assert_eq!(
        original_path
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        [user.id, first_answer.id]
    );
    let all_messages = database
        .list_all_conversation_messages(conversation.id)
        .await
        .unwrap();
    assert_eq!(all_messages.len(), 3);
    assert_eq!(
        database
            .list_conversation_branches(conversation.id)
            .await
            .unwrap()
            .len(),
        2
    );

    let restored = database
        .activate_conversation_branch(conversation.id, conversation.default_branch_id)
        .await
        .unwrap();
    assert_eq!(restored.default_branch_id, conversation.default_branch_id);
}

#[tokio::test]
async fn creates_durable_idempotent_split_chat_lanes_from_one_user_head() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Split comparison")
        .await
        .unwrap();
    let user = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("Compare this prompt")],
        })
        .await
        .unwrap();
    database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: Some(user.id),
            role: MessageRole::Assistant,
            parts: vec![text_part("Original answer")],
        })
        .await
        .unwrap();
    let selections = vec![
        ModelSelection {
            provider: AiProvider::LocalMistralRs,
            model_id: "org/fast".to_string(),
            model_alias: "fast-local".to_string(),
            load_profile: ModelLoadProfile::default(),
        },
        ModelSelection {
            provider: AiProvider::LocalMistralRs,
            model_id: "org/quality".to_string(),
            model_alias: "fast-local".to_string(),
            load_profile: ModelLoadProfile {
                quantization: ModelQuantization::Q4K,
                ..ModelLoadProfile::default()
            },
        },
    ];

    let split = database
        .create_split_chat_idempotent(
            conversation.id,
            conversation.default_branch_id,
            user.id,
            &selections,
            Some("split-request-1"),
        )
        .await
        .unwrap();
    assert_eq!(split.lanes.len(), 2);
    assert_eq!(split.input_message_id, user.id);
    assert_ne!(split.lanes[0].branch_id, split.lanes[1].branch_id);
    for (lane, selection) in split.lanes.iter().zip(&selections) {
        assert_eq!(lane.generation.branch_id, lane.branch_id);
        assert_eq!(lane.generation.input_message_id, Some(user.id));
        assert_eq!(
            lane.generation.model_id.as_deref(),
            Some(selection.model_id.as_str())
        );
        assert_eq!(
            lane.generation.load_profile.as_ref(),
            Some(&selection.load_profile)
        );
        assert_eq!(
            database
                .list_messages(conversation.id, lane.branch_id)
                .await
                .unwrap()
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            [user.id]
        );
    }
    assert_eq!(
        database
            .get_conversation(conversation.id)
            .await
            .unwrap()
            .default_branch_id,
        conversation.default_branch_id
    );

    let retried = database
        .create_split_chat_idempotent(
            conversation.id,
            conversation.default_branch_id,
            user.id,
            &selections,
            Some("split-request-1"),
        )
        .await
        .unwrap();
    assert_eq!(retried, split);
    assert_eq!(
        database.list_split_chats(conversation.id).await.unwrap(),
        [split]
    );

    let conflicting_selections = vec![selections[1].clone(), selections[0].clone()];
    let conflicting = database
        .create_split_chat_idempotent(
            conversation.id,
            conversation.default_branch_id,
            user.id,
            &conflicting_selections,
            Some("split-request-1"),
        )
        .await;
    assert!(matches!(conflicting, Err(StorageError::Conflict(_))));
}

#[tokio::test]
async fn allocates_generation_sequences_atomically() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Events")
        .await
        .unwrap();
    let run = database
        .create_generation_run(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
        )
        .await
        .unwrap();
    let mut tasks = JoinSet::new();
    for index in 0..32 {
        let database = database.clone();
        tasks.spawn(async move {
            database
                .append_generation_event(run.id, "delta", json!({ "index": index }))
                .await
                .unwrap()
                .sequence_number
        });
    }
    let mut sequences = Vec::with_capacity(32);
    while let Some(result) = tasks.join_next().await {
        sequences.push(result.unwrap());
    }
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=32).collect::<Vec<_>>());

    let events = database
        .list_generation_events(run.id, 24, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 8);
    assert_eq!(events[0].sequence_number, 25);
    assert_eq!(events[7].sequence_number, 32);
    database
        .set_generation_status(run.id, GenerationStatus::Completed)
        .await
        .unwrap();
    let stored = database.get_generation_run(run.id).await.unwrap();
    assert_eq!(stored.last_sequence, 32);
    assert_eq!(stored.status, GenerationStatus::Completed);
}

#[tokio::test]
async fn deduplicates_generation_creation_by_client_request_id() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Idempotency")
        .await
        .unwrap();
    let user = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("Generate once")],
        })
        .await
        .unwrap();

    let first = database
        .create_generation_run_idempotent(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
            Some("request-1"),
        )
        .await
        .unwrap();
    let replay = database
        .create_generation_run_idempotent(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
            Some("request-1"),
        )
        .await
        .unwrap();

    assert_eq!(first.id, replay.id);
    assert_eq!(first.input_message_id, Some(user.id));
    assert_eq!(first.client_request_id.as_deref(), Some("request-1"));
    assert!(
        database
            .create_generation_run_idempotent(
                conversation.id,
                conversation.default_branch_id,
                "different-model",
                Some("request-1"),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn commits_generation_output_and_terminal_event_atomically() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Lifecycle")
        .await
        .unwrap();
    let user = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("Say hello")],
        })
        .await
        .unwrap();
    let run = database
        .create_generation_run(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
        )
        .await
        .unwrap();
    assert_eq!(run.input_message_id, Some(user.id));
    assert_eq!(run.output_message_id, None);

    let started = database.start_generation_run(run.id).await.unwrap();
    assert_eq!(started.sequence_number, 1);
    database
        .append_generation_event(
            run.id,
            "request_metrics",
            json!({ "messageCount": 1, "inputCharacters": 9 }),
        )
        .await
        .unwrap();
    database
        .append_generation_event(run.id, "delta", json!({ "text": "Hello" }))
        .await
        .unwrap();
    database
        .append_generation_event(
            run.id,
            "usage",
            json!({
                "prompt_tokens": 2,
                "completion_tokens": 3,
                "total_tokens": 5
            }),
        )
        .await
        .unwrap();
    let (assistant, completed) = database
        .complete_generation_with_text(run.id, "Hello there".to_string())
        .await
        .unwrap();
    assert_eq!(assistant.parent_message_id, Some(user.id));
    assert_eq!(assistant.role, MessageRole::Assistant);
    assert_eq!(completed.sequence_number, 5);
    assert_eq!(completed.event_type, "completed");

    let stored = database.get_generation_run(run.id).await.unwrap();
    assert_eq!(stored.status, GenerationStatus::Completed);
    assert_eq!(stored.output_message_id, Some(assistant.id));
    assert_eq!(stored.last_sequence, 5);
    let performance = database.generation_performance_samples(20).await.unwrap();
    assert_eq!(performance.len(), 1);
    assert_eq!(performance[0].generation_id, run.id);
    assert_eq!(performance[0].input_characters, Some(9));
    assert_eq!(performance[0].output_characters, 5);
    assert_eq!(performance[0].prompt_tokens, Some(2));
    assert_eq!(performance[0].completion_tokens, Some(3));
    let messages = database
        .list_messages(conversation.id, conversation.default_branch_id)
        .await
        .unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].parts[0].content, "Hello there");
    assert!(
        database
            .finalize_generation(
                run.id,
                GenerationStatus::Cancelled,
                "cancelled",
                json!({ "reason": "late" }),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancellation_is_idempotently_terminal_without_an_output_message() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Cancel")
        .await
        .unwrap();
    let run = database
        .create_generation_run(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
        )
        .await
        .unwrap();
    let cancelled = database
        .finalize_generation(
            run.id,
            GenerationStatus::Cancelled,
            "cancelled",
            json!({ "reason": "user_requested" }),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.sequence_number, 1);
    let stored = database.get_generation_run(run.id).await.unwrap();
    assert_eq!(stored.status, GenerationStatus::Cancelled);
    assert_eq!(stored.output_message_id, None);
}

#[tokio::test]
async fn cancellation_persists_partial_assistant_output_atomically() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Partial cancel")
        .await
        .unwrap();
    let user = database
        .append_message(NewMessage {
            conversation_id: conversation.id,
            branch_id: conversation.default_branch_id,
            parent_message_id: None,
            role: MessageRole::User,
            parts: vec![text_part("Start a long answer")],
        })
        .await
        .unwrap();
    let run = database
        .create_generation_run(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
        )
        .await
        .unwrap();
    database.start_generation_run(run.id).await.unwrap();
    assert_eq!(
        database
            .list_active_generation_runs(conversation.id)
            .await
            .unwrap()
            .len(),
        1
    );
    let (assistant, cancelled) = database
        .cancel_generation_with_text(run.id, "A durable partial".to_string())
        .await
        .unwrap();

    assert_eq!(assistant.parent_message_id, Some(user.id));
    assert_eq!(assistant.parts[0].content, "A durable partial");
    assert_eq!(
        assistant.parts[0].metadata,
        Some(json!({ "unfinished": true }))
    );
    assert_eq!(cancelled.event_type, "cancelled");
    assert_eq!(
        cancelled.payload["outputMessageId"],
        assistant.id.to_string()
    );
    assert_eq!(cancelled.payload["unfinished"], true);
    let stored = database.get_generation_run(run.id).await.unwrap();
    assert_eq!(stored.status, GenerationStatus::Cancelled);
    assert_eq!(stored.output_message_id, Some(assistant.id));
    assert!(
        database
            .list_active_generation_runs(conversation.id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recovers_running_generations_after_a_core_restart() {
    let (database, _directory) = test_database().await;
    let conversation = database
        .create_conversation(workspace_id(), "Recovery")
        .await
        .unwrap();
    let run = database
        .create_generation_run(
            conversation.id,
            conversation.default_branch_id,
            "fast-local",
        )
        .await
        .unwrap();
    database.start_generation_run(run.id).await.unwrap();

    let recovered = database.recover_interrupted_generations().await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].generation_id, run.id);
    assert_eq!(recovered[0].sequence_number, 2);
    assert_eq!(recovered[0].payload["code"], "core_restarted");
    let stored = database.get_generation_run(run.id).await.unwrap();
    assert_eq!(stored.status, GenerationStatus::Failed);
    assert_eq!(stored.last_sequence, 2);
}

#[tokio::test]
async fn knowledge_documents_keep_fts_embeddings_and_dense_journal_transactionally_aligned() {
    let (database, _directory) = test_database().await;
    assert_eq!(database.schema_version().await.unwrap(), 11);
    let stack = database
        .create_knowledge_stack(NewKnowledgeStack {
            workspace_id: workspace_id(),
            name: "Release engineering".to_string(),
            description: "Local runbooks".to_string(),
            embedding_model: "test/embedding".to_string(),
            embedding_dimensions: 8,
            chunk_size: 256,
            chunk_overlap: 32,
            chunker_hash: test_hash('a'),
        })
        .await
        .unwrap();

    let first = database
        .upsert_knowledge_document(NewKnowledgeDocument {
            stack_id: stack.id,
            workspace_id: stack.workspace_id,
            source_kind: KnowledgeSourceKind::File,
            source_uri: "file:///runbook.md".to_string(),
            title: "Release runbook".to_string(),
            mime_type: "text/markdown".to_string(),
            content_sha256: test_hash('b'),
            blob_sha256: test_hash('c'),
            byte_size: 48,
            source_modified_at_ms: Some(1),
            chunks: vec![
                NewKnowledgeChunk {
                    id: Uuid::now_v7(),
                    ordinal: 0,
                    content: "Deploy the aurora service after the database migration.".to_string(),
                    content_sha256: test_hash('d'),
                    character_count: 56,
                    start_offset: 0,
                    end_offset: 56,
                    metadata: json!({ "section": "deploy" }),
                    embedding: vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    vector_sha256: test_hash('e'),
                },
                NewKnowledgeChunk {
                    id: Uuid::now_v7(),
                    ordinal: 1,
                    content: "Rollback uses the cobalt snapshot.".to_string(),
                    content_sha256: test_hash('f'),
                    character_count: 34,
                    start_offset: 57,
                    end_offset: 91,
                    metadata: json!({ "section": "rollback" }),
                    embedding: vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    vector_sha256: test_hash('1'),
                },
            ],
        })
        .await
        .unwrap();
    let first_chunks = database.list_document_chunks(first.id).await.unwrap();
    let old_dense_ids = first_chunks
        .iter()
        .map(|chunk| chunk.dense_id)
        .collect::<Vec<_>>();
    assert_eq!(
        database.all_stack_embeddings(stack.id).await.unwrap().len(),
        2
    );
    assert_eq!(
        database
            .search_knowledge_chunks_lexical(stack.workspace_id, stack.id, "aurora", 20)
            .await
            .unwrap()
            .len(),
        1
    );
    let first_journal = database.dense_journal_after(stack.id, 0).await.unwrap();
    assert_eq!(first_journal.len(), 2);
    assert!(
        first_journal
            .iter()
            .all(|entry| entry.revision == 1 && entry.operation == "upsert")
    );

    let updated = database
        .upsert_knowledge_document(NewKnowledgeDocument {
            stack_id: stack.id,
            workspace_id: stack.workspace_id,
            source_kind: KnowledgeSourceKind::File,
            source_uri: "file:///runbook.md".to_string(),
            title: "Release runbook v2".to_string(),
            mime_type: "text/markdown".to_string(),
            content_sha256: test_hash('2'),
            blob_sha256: test_hash('3'),
            byte_size: 41,
            source_modified_at_ms: Some(2),
            chunks: vec![NewKnowledgeChunk {
                id: Uuid::now_v7(),
                ordinal: 0,
                content: "The zephyr release now uses a canary.".to_string(),
                content_sha256: test_hash('4'),
                character_count: 37,
                start_offset: 0,
                end_offset: 37,
                metadata: json!({ "section": "canary" }),
                embedding: vec![0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                vector_sha256: test_hash('5'),
            }],
        })
        .await
        .unwrap();
    assert_eq!(updated.id, first.id);
    assert!(
        database
            .search_knowledge_chunks_lexical(stack.workspace_id, stack.id, "aurora", 20)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .search_knowledge_chunks_lexical(stack.workspace_id, stack.id, "zephyr", 20)
            .await
            .unwrap()
            .len(),
        1
    );
    let second_journal = database.dense_journal_after(stack.id, 1).await.unwrap();
    assert_eq!(
        second_journal
            .iter()
            .filter(|entry| entry.operation == "remove")
            .count(),
        2
    );
    assert_eq!(
        second_journal
            .iter()
            .filter(|entry| entry.operation == "upsert")
            .count(),
        1
    );
    assert!(old_dense_ids.iter().all(|id| {
        second_journal
            .iter()
            .any(|entry| entry.dense_id == *id && entry.operation == "remove")
    }));

    assert!(
        database
            .delete_knowledge_document(updated.id)
            .await
            .unwrap()
    );
    assert!(
        database
            .allowed_knowledge_dense_ids(stack.workspace_id, stack.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .all_stack_embeddings(stack.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .knowledge_stack(stack.id)
            .await
            .unwrap()
            .index_revision,
        3
    );
}

#[tokio::test]
async fn generation_knowledge_selection_is_atomic_and_idempotent() {
    let (database, _directory) = test_database().await;
    let stack = database
        .create_knowledge_stack(NewKnowledgeStack {
            workspace_id: workspace_id(),
            name: "Grounding".to_string(),
            description: String::new(),
            embedding_model: "test/embedding".to_string(),
            embedding_dimensions: 8,
            chunk_size: 256,
            chunk_overlap: 32,
            chunker_hash: test_hash('9'),
        })
        .await
        .unwrap();
    let conversation = database
        .create_conversation(workspace_id(), "Knowledge idempotency")
        .await
        .unwrap();
    let selection = ModelSelection {
        provider: AiProvider::LocalMistralRs,
        model_id: "test/generator".to_string(),
        model_alias: "local-default".to_string(),
        load_profile: ModelLoadProfile::default(),
    };

    assert!(
        database
            .create_generation_run_for_model_with_knowledge_idempotent(
                conversation.id,
                conversation.default_branch_id,
                &selection,
                Some("missing-stack"),
                &[Uuid::now_v7()],
            )
            .await
            .is_err()
    );
    assert!(
        database
            .list_active_generation_runs(conversation.id)
            .await
            .unwrap()
            .is_empty()
    );

    let run = database
        .create_generation_run_for_model_with_knowledge_idempotent(
            conversation.id,
            conversation.default_branch_id,
            &selection,
            Some("grounded-run"),
            &[stack.id],
        )
        .await
        .unwrap();
    let retry = database
        .create_generation_run_for_model_with_knowledge_idempotent(
            conversation.id,
            conversation.default_branch_id,
            &selection,
            Some("grounded-run"),
            &[stack.id],
        )
        .await
        .unwrap();
    assert_eq!(retry.id, run.id);
    assert_eq!(
        database.generation_knowledge_stacks(run.id).await.unwrap(),
        vec![stack.id]
    );
    let conflict = database
        .create_generation_run_for_model_with_knowledge_idempotent(
            conversation.id,
            conversation.default_branch_id,
            &selection,
            Some("grounded-run"),
            &[],
        )
        .await;
    assert!(matches!(conflict, Err(StorageError::Conflict(_))));
    assert_eq!(
        database.generation_knowledge_stacks(run.id).await.unwrap(),
        vec![stack.id]
    );
}
