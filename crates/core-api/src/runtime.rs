use std::sync::{Arc, RwLock};

use northbridge_ai_gateway::LocalAiRuntime;
use northbridge_domain::{
    AiProvider, ModelCatalogEntry, ModelLoadProfile, ModelQuantization, ModelSelection,
};

pub const LOCAL_MODEL_ALIAS: &str = "fast-local";

#[derive(Clone)]
pub(crate) struct SelectedAiRuntime {
    pub(crate) selection: ModelSelection,
    pub(crate) runtime: LocalAiRuntime,
}

#[derive(Clone, Default)]
pub(crate) struct AiRuntimeRegistry {
    inner: Arc<RwLock<AiRuntimeRegistryState>>,
}

#[derive(Default)]
struct AiRuntimeRegistryState {
    current: Option<SelectedAiRuntime>,
    pooled: Vec<SelectedAiRuntime>,
}

impl AiRuntimeRegistry {
    pub(crate) fn current(&self) -> Option<SelectedAiRuntime> {
        read_state(&self.inner).current.clone()
    }

    pub(crate) fn selection(&self) -> Option<ModelSelection> {
        read_state(&self.inner)
            .current
            .as_ref()
            .map(|selected| selected.selection.clone())
    }

    pub(crate) fn for_selection(&self, selection: &ModelSelection) -> Option<SelectedAiRuntime> {
        let state = read_state(&self.inner);
        state
            .current
            .iter()
            .chain(&state.pooled)
            .find(|selected| &selected.selection == selection)
            .cloned()
    }

    pub(crate) fn uses_model(&self, model_id: &str) -> bool {
        let state = read_state(&self.inner);
        state
            .current
            .iter()
            .chain(&state.pooled)
            .any(|selected| selected.selection.model_id == model_id)
    }

    pub(crate) fn replace(
        &self,
        selection: ModelSelection,
        runtime: LocalAiRuntime,
    ) -> Vec<SelectedAiRuntime> {
        let mut state = write_state(&self.inner);
        let mut replaced = state.pooled.drain(..).collect::<Vec<_>>();
        if let Some(current) = state
            .current
            .replace(SelectedAiRuntime { selection, runtime })
        {
            replaced.push(current);
        }
        replaced
    }

    pub(crate) fn add_pooled(
        &self,
        selection: ModelSelection,
        runtime: LocalAiRuntime,
    ) -> Option<LocalAiRuntime> {
        let mut state = write_state(&self.inner);
        if state
            .current
            .iter()
            .chain(&state.pooled)
            .any(|selected| selected.selection == selection)
        {
            return Some(runtime);
        }
        state.pooled.push(SelectedAiRuntime { selection, runtime });
        None
    }

    pub(crate) fn take_all(&self) -> Vec<SelectedAiRuntime> {
        let mut state = write_state(&self.inner);
        let mut selected = state.pooled.drain(..).collect::<Vec<_>>();
        selected.extend(state.current.take());
        selected
    }
}

#[must_use]
pub fn local_model_catalog() -> Vec<ModelCatalogEntry> {
    vec![
        ModelCatalogEntry {
            provider: AiProvider::LocalMistralRs,
            model_id: "HuggingFaceTB/SmolLM2-135M-Instruct".to_string(),
            model_alias: LOCAL_MODEL_ALIAS.to_string(),
            display_name: "SmolLM2 135M Instruct".to_string(),
            description:
                "Smallest verified CPU starter model; optimized for a quick first local chat."
                    .to_string(),
            context_window: 8192,
            recommended: true,
        },
        ModelCatalogEntry {
            provider: AiProvider::LocalMistralRs,
            model_id: "HuggingFaceTB/SmolLM2-360M-Instruct".to_string(),
            model_alias: LOCAL_MODEL_ALIAS.to_string(),
            display_name: "SmolLM2 360M Instruct".to_string(),
            description: "Larger CPU-friendly starter model with improved response quality."
                .to_string(),
            context_window: 8192,
            recommended: false,
        },
    ]
}

#[must_use]
pub fn local_model_selection(model_id: &str) -> Option<ModelSelection> {
    local_model_selection_with_profile(model_id, ModelLoadProfile::default())
}

#[must_use]
pub fn local_model_selection_with_profile(
    model_id: &str,
    load_profile: ModelLoadProfile,
) -> Option<ModelSelection> {
    if load_profile.validate().is_err() {
        return None;
    }
    local_model_catalog()
        .into_iter()
        .find(|entry| entry.model_id == model_id)
        .map(|entry| ModelSelection {
            provider: entry.provider,
            model_id: entry.model_id,
            model_alias: entry.model_alias,
            load_profile,
        })
}

#[must_use]
pub fn custom_local_model_selection(model_id: impl Into<String>) -> Option<ModelSelection> {
    let model_id = model_id.into();
    let trimmed = model_id.trim();
    if trimmed.is_empty() || trimmed.len() > 512 {
        return None;
    }
    Some(ModelSelection {
        provider: AiProvider::LocalMistralRs,
        model_id: trimmed.to_string(),
        model_alias: LOCAL_MODEL_ALIAS.to_string(),
        load_profile: ModelLoadProfile::default(),
    })
}

#[must_use]
pub const fn gateway_quantization(
    quantization: ModelQuantization,
) -> Option<northbridge_ai_gateway::MistralQuantization> {
    match quantization {
        ModelQuantization::None => None,
        ModelQuantization::Q2K => Some(northbridge_ai_gateway::MistralQuantization::Q2K),
        ModelQuantization::Q3K => Some(northbridge_ai_gateway::MistralQuantization::Q3K),
        ModelQuantization::Q4K => Some(northbridge_ai_gateway::MistralQuantization::Q4K),
        ModelQuantization::Q5K => Some(northbridge_ai_gateway::MistralQuantization::Q5K),
        ModelQuantization::Q6K => Some(northbridge_ai_gateway::MistralQuantization::Q6K),
        ModelQuantization::Q8Zero => Some(northbridge_ai_gateway::MistralQuantization::Q8Zero),
    }
}

#[must_use]
pub fn gateway_runtime_profile(
    profile: &ModelLoadProfile,
) -> northbridge_ai_gateway::MistralRuntimeProfile {
    northbridge_ai_gateway::MistralRuntimeProfile {
        quantization: gateway_quantization(profile.quantization),
        force_cpu: profile.force_cpu,
        max_sequence_length: profile.max_sequence_length,
        max_batch_size: profile.max_batch_size,
    }
}

fn read_state(
    state: &RwLock<AiRuntimeRegistryState>,
) -> std::sync::RwLockReadGuard<'_, AiRuntimeRegistryState> {
    state
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_state(
    state: &RwLock<AiRuntimeRegistryState>,
) -> std::sync::RwLockWriteGuard<'_, AiRuntimeRegistryState> {
    state
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{LOCAL_MODEL_ALIAS, custom_local_model_selection, local_model_catalog};

    #[test]
    fn catalog_has_one_recommended_model_and_stable_aliases() {
        let catalog = local_model_catalog();
        assert!(catalog.len() >= 2);
        assert_eq!(catalog.iter().filter(|model| model.recommended).count(), 1);
        assert!(
            catalog
                .iter()
                .all(|model| model.model_alias == LOCAL_MODEL_ALIAS)
        );
    }

    #[test]
    fn custom_selection_is_bounded() {
        assert!(custom_local_model_selection("org/model").is_some());
        assert!(custom_local_model_selection("   ").is_none());
        assert!(custom_local_model_selection("x".repeat(513)).is_none());
    }
}
