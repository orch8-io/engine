//! Encrypting storage decorator.
//!
//! Wraps any `StorageBackend` and transparently encrypts `context.data` on write
//! and decrypts on read using AES-256-GCM via [`FieldEncryptor`].

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::context::ExecutionContext;
use orch8_types::encryption::FieldEncryptor;
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::TaskInstance;

use crate::StorageBackend;

/// Magic header prepended to encrypted artifact blobs (`"O8ENC" || 0x01`).
/// Lets reads distinguish an encrypted blob from one written before encryption
/// was enabled, so toggling the key on doesn't render pre-existing plaintext
/// blobs unreadable. Distinctive enough that a plaintext blob is extremely
/// unlikely to start with it.
const ARTIFACT_ENC_MAGIC: &[u8] = b"O8ENC\x01";

/// Wraps a whole `impl SomeTrait for EncryptingStorage { ... }` block, where
/// each method is either:
/// - a bare signature ending in `;` -- expands to a pure pass-through
///   (`self.inner.$name(args).await`), the shape most of this decorator's
///   methods take, since most `StorageBackend` methods never touch
///   `context.data` (the only field this wrapper encrypts); or
/// - a signature with a `{ ... }` body -- a hand-written encrypting
///   override, emitted verbatim.
///
/// Collapses what would otherwise be a several-line `async fn foo(&self,
/// ...) -> R { self.inner.foo(...).await }` block into one line per
/// pass-through method, and keeps `self.inner.$name` textually present in
/// the expansion so a `grep self.inner.` coverage audit (the technique that
/// found finding M1 in the 2026-07 storage review) still surfaces every
/// pass-through, macro-expanded or not.
///
/// This is *not* a substitute for R1's original proposal (an
/// exhaustiveness-checked delegation macro via `ambassador`) -- that crate
/// turned out to be incompatible with `EncryptingStorage.inner` being a
/// trait object (`Arc<dyn StorageBackend>` never satisfies the generic
/// `Target: Trait` bound `ambassador`'s derive requires, only method-call
/// syntax gets the vtable dispatch). A newly-added trait method is *not*
/// automatically caught by this macro -- it still requires a human to add a
/// `;`-terminated line (or a hand-written encrypting override) for it.
///
/// Implementation note: this macro expands to literal `async fn` items and
/// applies `#[async_trait]` to the *result*, rather than each method calling
/// a smaller per-method macro from inside an already-`#[async_trait]`-tagged
/// block. `#[async_trait]` rewrites the literal `async fn` syntax it sees at
/// its own expansion time, which runs before any macro invocations nested
/// inside the block are expanded -- a per-method macro call would still look
/// like an opaque, non-`async fn` item to it and be left untransformed
/// (hand-rolling `#[async_trait]`'s lifetime-elaborated desugaring by hand
/// was tried and abandoned: every reference-typed argument needs its own
/// named lifetime bound to `'async_trait`, which a macro operating on
/// already-captured `$ty:ty` fragments can't inject generically). Expanding
/// to the whole block first sidesteps the ordering problem -- almost: a
/// naive single-pass expansion that itself recursively invokes
/// `passthrough_impl!` *from inside* the `#[async_trait]`-tagged block hits
/// the exact same issue one level deeper (the attribute still sees an
/// unexpanded nested macro call, not `async fn` syntax). The fix is the
/// standard "TT-muncher with accumulator" shape below: each `@munch` step
/// recurses via its own top-level macro invocation (not one nested inside an
/// attributed item), building the fully-expanded method list up in the `[$(
/// $methods:tt)*]` accumulator, and only the base case -- once every method
/// has already been expanded to literal `async fn` tokens -- emits the
/// `#[async_trait] impl { ... }` block. By then there is no macro call left
/// inside it for the attribute to fail to see.
macro_rules! passthrough_impl {
    (impl crate::$trait_name:ident for EncryptingStorage { $($body:tt)* }) => {
        passthrough_impl!(@munch [$trait_name] [] $($body)*);
    };

    // Base case: every method already expanded into the accumulator.
    (@munch [$trait_name:ident] [$($methods:tt)*]) => {
        #[async_trait]
        impl crate::$trait_name for EncryptingStorage {
            $($methods)*
        }
    };

    // Pass-through: bare signature terminated by `;`.
    (@munch [$trait_name:ident] [$($methods:tt)*] async fn $name:ident(&self $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty ; $($rest:tt)*) => {
        passthrough_impl!(@munch [$trait_name] [$($methods)* async fn $name(&self, $($arg: $ty),*) -> $ret {
            self.inner.$name($($arg),*).await
        }] $($rest)*);
    };

    // Hand-written override: signature with an inline body, emitted as-is.
    //
    // Captures `self` via `&$self_tok:tt` rather than matching the literal
    // `&self` (as the pass-through arm above does) -- the body is emitted
    // verbatim from the call site's tokens, so its `self.` references carry
    // the call site's hygiene context. Re-emitting a *literal* `self` here
    // (as the pass-through arm safely can, since it also writes the body
    // itself) would mint a hygienically distinct `self`, and `#[async_trait]`'s
    // `self` -> `__self` rewrite then only renames one of the two, leaving
    // the other unresolved ("cannot find value `__self`"). Re-emitting the
    // captured token instead preserves its original identity.
    (@munch [$trait_name:ident] [$($methods:tt)*] async fn $name:ident(&$self_tok:tt $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty { $($fn_body:tt)* } $($rest:tt)*) => {
        passthrough_impl!(@munch [$trait_name] [$($methods)* async fn $name(&$self_tok, $($arg: $ty),*) -> $ret { $($fn_body)* }] $($rest)*);
    };

    // Non-async override (e.g. a trait method that isn't `async fn`, like
    // `ResourceStore::artifacts_enabled`). `#[async_trait]` only transforms
    // `async fn` items and passes plain `fn` items through untouched -- but
    // the same `self`-hygiene concern as the async override arm still
    // applies (the body's `self` comes from the call site), so `self` is
    // still captured via `&$self_tok:tt` rather than matched literally.
    (@munch [$trait_name:ident] [$($methods:tt)*] fn $name:ident(&$self_tok:tt $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty { $($fn_body:tt)* } $($rest:tt)*) => {
        passthrough_impl!(@munch [$trait_name] [$($methods)* fn $name(&$self_tok, $($arg: $ty),*) -> $ret { $($fn_body)* }] $($rest)*);
    };
}

/// Wraps an inner `StorageBackend` and encrypts/decrypts `context.data` transparently.
pub struct EncryptingStorage {
    inner: Arc<dyn StorageBackend>,
    encryptor: FieldEncryptor,
}

impl EncryptingStorage {
    pub fn new(inner: Arc<dyn StorageBackend>, encryptor: FieldEncryptor) -> Self {
        Self { inner, encryptor }
    }

    /// Associated data binding a `context.data` ciphertext to the instance it
    /// belongs to: the raw 16 UUID bytes of its `InstanceId`. Passed as AAD
    /// to AES-GCM so a ciphertext copied to a different row (or a different
    /// tenant's row, e.g. by an attacker with DB write access) fails to
    /// decrypt there instead of silently succeeding -- see the deep storage
    /// review's finding on AES-GCM ciphertext binding.
    fn instance_aad(id: InstanceId) -> [u8; 16] {
        *id.into_uuid().as_bytes()
    }

    /// Encrypt `context.data` on a `TaskInstance`, returning a `Cow` so callers
    /// can pass the result straight through when encryption is unnecessary.
    ///
    /// If `context.data` already carries an `enc:` prefix, the instance is
    /// returned borrowed (no clone, no re-encryption -- re-encrypting would
    /// produce a layered payload that `decrypt_value` cannot unwrap in a
    /// single pass, silently corrupting round-trips).
    fn encrypt_instance<'a>(
        &self,
        instance: &'a TaskInstance,
    ) -> Result<Cow<'a, TaskInstance>, StorageError> {
        if FieldEncryptor::is_encrypted(&instance.context.data) {
            return Ok(Cow::Borrowed(instance));
        }
        let mut inst = instance.clone();
        let aad = Self::instance_aad(instance.id);
        inst.context.data = self
            .encryptor
            .encrypt_value_with_aad(&inst.context.data, &aad)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        Ok(Cow::Owned(inst))
    }

    /// Encrypt `context.data` on an `ExecutionContext`, bound to `id` via AAD.
    /// Same guard as `encrypt_instance`: already-encrypted payloads
    /// short-circuit with a borrowed reference.
    fn encrypt_context<'a>(
        &self,
        id: InstanceId,
        context: &'a ExecutionContext,
    ) -> Result<Cow<'a, ExecutionContext>, StorageError> {
        if FieldEncryptor::is_encrypted(&context.data) {
            return Ok(Cow::Borrowed(context));
        }
        let mut ctx = context.clone();
        let aad = Self::instance_aad(id);
        ctx.data = self
            .encryptor
            .encrypt_value_with_aad(&ctx.data, &aad)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        Ok(Cow::Owned(ctx))
    }

    /// Encrypt `context.data` on an `ExecutionContext` in-place (bound to
    /// `id` via AAD), avoiding clones.
    fn encrypt_context_mut(
        &self,
        id: InstanceId,
        context: &mut ExecutionContext,
    ) -> Result<(), StorageError> {
        if !FieldEncryptor::is_encrypted(&context.data) {
            let aad = Self::instance_aad(id);
            context.data = self
                .encryptor
                .encrypt_value_with_aad(&context.data, &aad)
                .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        }
        Ok(())
    }

    /// Encrypt an arbitrary JSON value (used for externalized-state payloads,
    /// which are not part of `context.data` but hold the same class of data
    /// once a field has been swapped out for a marker). No-op if already
    /// encrypted, mirroring `encrypt_context`'s double-encryption guard.
    fn encrypt_json_value(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, StorageError> {
        if FieldEncryptor::is_encrypted(value) {
            return Ok(value.clone());
        }
        self.encryptor
            .encrypt_value(value)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))
    }

    /// Decrypt an arbitrary JSON value produced by `encrypt_json_value`.
    /// Returns the input unchanged if it was never encrypted (payloads
    /// written before encryption was enabled stay readable).
    fn decrypt_json_value(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, StorageError> {
        if FieldEncryptor::is_encrypted(value) {
            return self
                .encryptor
                .decrypt_value(value)
                .map_err(|e| StorageError::Query(format!("encryption: {e}")));
        }
        Ok(value.clone())
    }

    /// Encrypt `BlockOutput.output` -- handler results (LLM responses, HTTP
    /// bodies, etc.) are the same data class as `context.data`, so they get
    /// the same at-rest protection. Returns a borrowed `Cow` when the value
    /// is already encrypted (e.g. a copy/retry of a previously-saved output).
    fn encrypt_block_output<'a>(
        &self,
        output: &'a orch8_types::output::BlockOutput,
    ) -> Result<Cow<'a, orch8_types::output::BlockOutput>, StorageError> {
        if FieldEncryptor::is_encrypted(&output.output) {
            return Ok(Cow::Borrowed(output));
        }
        let mut o = output.clone();
        o.output = self.encrypt_json_value(&o.output)?;
        Ok(Cow::Owned(o))
    }

    /// Decrypt `BlockOutput.output` in place after a read.
    fn decrypt_block_output(
        &self,
        output: &mut orch8_types::output::BlockOutput,
    ) -> Result<(), StorageError> {
        output.output = self.decrypt_json_value(&output.output)?;
        Ok(())
    }

    /// Encrypt `Signal.payload`. Update-context signals carry a full
    /// `ExecutionContext` snapshot -- the same data class as `context.data`
    /// -- so the queued payload gets the same protection rather than sitting
    /// in `signal_inbox` as plaintext until delivery.
    fn encrypt_signal<'a>(
        &self,
        signal: &'a orch8_types::signal::Signal,
    ) -> Result<Cow<'a, orch8_types::signal::Signal>, StorageError> {
        if FieldEncryptor::is_encrypted(&signal.payload) {
            return Ok(Cow::Borrowed(signal));
        }
        let mut s = signal.clone();
        s.payload = self.encrypt_json_value(&s.payload)?;
        Ok(Cow::Owned(s))
    }

    /// Decrypt `Signal.payload` in place after a read.
    fn decrypt_signal(&self, signal: &mut orch8_types::signal::Signal) -> Result<(), StorageError> {
        signal.payload = self.decrypt_json_value(&signal.payload)?;
        Ok(())
    }

    /// Encrypt a `WorkerTask`'s `params`, `context` (a serialized
    /// `ExecutionContext` snapshot dispatched to external workers) and
    /// `output` fields. Same data class as `context.data` -- an external
    /// worker dispatch shouldn't hand a decrypted context snapshot to a
    /// third-party process while at-rest storage is encrypted.
    fn encrypt_worker_task<'a>(
        &self,
        task: &'a orch8_types::worker::WorkerTask,
    ) -> Result<Cow<'a, orch8_types::worker::WorkerTask>, StorageError> {
        if FieldEncryptor::is_encrypted(&task.params)
            && FieldEncryptor::is_encrypted(&task.context)
            && task
                .output
                .as_ref()
                .is_none_or(FieldEncryptor::is_encrypted)
        {
            return Ok(Cow::Borrowed(task));
        }
        let mut t = task.clone();
        t.params = self.encrypt_json_value(&t.params)?;
        t.context = self.encrypt_json_value(&t.context)?;
        if let Some(ref output) = t.output {
            t.output = Some(self.encrypt_json_value(output)?);
        }
        Ok(Cow::Owned(t))
    }

    /// Decrypt a `WorkerTask`'s `params`, `context` and `output` fields in place.
    fn decrypt_worker_task(
        &self,
        task: &mut orch8_types::worker::WorkerTask,
    ) -> Result<(), StorageError> {
        task.params = self.decrypt_json_value(&task.params)?;
        task.context = self.decrypt_json_value(&task.context)?;
        if let Some(ref output) = task.output {
            task.output = Some(self.decrypt_json_value(output)?);
        }
        Ok(())
    }

    fn decrypt_instance(&self, instance: &mut TaskInstance) -> Result<(), StorageError> {
        if FieldEncryptor::is_encrypted(&instance.context.data) {
            let aad = Self::instance_aad(instance.id);
            instance.context.data = self
                .encryptor
                .decrypt_value_with_aad(&instance.context.data, &aad)
                .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        }
        Ok(())
    }

    fn decrypt_instances(&self, instances: &mut [TaskInstance]) -> Result<(), StorageError> {
        for inst in instances.iter_mut() {
            self.decrypt_instance(inst)?;
        }
        Ok(())
    }

    /// Encrypt the `value` and `refresh_token` fields of a credential before storage.
    fn encrypt_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<orch8_types::credential::CredentialDef, StorageError> {
        let mut cred = credential.clone();
        let plain_value = serde_json::Value::String(cred.value.expose().to_string());
        // Guard against double-encryption (parity with encrypt_instance /
        // encrypt_context): a value already carrying the `enc:` magic must not
        // be wrapped again, or decrypt_credential would hand the handler an
        // `enc:…` blob as a live credential and the third-party call fails.
        if !orch8_types::encryption::FieldEncryptor::is_encrypted(&plain_value) {
            let encrypted_value = self
                .encryptor
                .encrypt_value(&plain_value)
                .map_err(|e| StorageError::Query(format!("credential encryption: {e}")))?;
            if let serde_json::Value::String(s) = encrypted_value {
                cred.value = orch8_types::config::SecretString::new(s);
            }
        }
        if let Some(ref rt) = cred.refresh_token {
            let plain_rt = serde_json::Value::String(rt.expose().to_string());
            if !orch8_types::encryption::FieldEncryptor::is_encrypted(&plain_rt) {
                let encrypted_rt = self
                    .encryptor
                    .encrypt_value(&plain_rt)
                    .map_err(|e| StorageError::Query(format!("credential encryption: {e}")))?;
                if let serde_json::Value::String(s) = encrypted_rt {
                    cred.refresh_token = Some(orch8_types::config::SecretString::new(s));
                }
            }
        }
        Ok(cred)
    }

    /// Decrypt the `value` and `refresh_token` fields of a credential after reading.
    ///
    /// Deliberately lenient (not [`FieldEncryptor::decrypt_value_strict`]):
    /// a credential written before encryption was enabled for this
    /// deployment is legitimately plaintext at rest, and must keep reading
    /// back unchanged (see `credential_plaintext_passthrough_on_read` in
    /// `orch8-storage/tests/encrypting_coverage.rs`) rather than erroring on
    /// every read until it happens to be re-saved.
    fn decrypt_credential(
        &self,
        credential: &mut orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        let val = serde_json::Value::String(credential.value.expose().to_string());
        if orch8_types::encryption::FieldEncryptor::is_encrypted(&val) {
            let decrypted = self
                .encryptor
                .decrypt_value(&val)
                .map_err(|e| StorageError::Query(format!("credential decryption: {e}")))?;
            if let serde_json::Value::String(s) = decrypted {
                credential.value = orch8_types::config::SecretString::new(s);
            }
        }
        if let Some(ref rt) = credential.refresh_token {
            let rt_val = serde_json::Value::String(rt.expose().to_string());
            if orch8_types::encryption::FieldEncryptor::is_encrypted(&rt_val) {
                let decrypted = self
                    .encryptor
                    .decrypt_value(&rt_val)
                    .map_err(|e| StorageError::Query(format!("credential decryption: {e}")))?;
                if let serde_json::Value::String(s) = decrypted {
                    credential.refresh_token = Some(orch8_types::config::SecretString::new(s));
                }
            }
        }
        Ok(())
    }

    /// Encrypt `TriggerDef.secret` (HMAC webhook key) before write. Other
    /// fields are returned unchanged.
    fn encrypt_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<orch8_types::trigger::TriggerDef, StorageError> {
        let mut t = trigger.clone();
        if let Some(secret) = t.secret.as_ref() {
            let enc = self.encrypt_string_field(secret.expose())?;
            t.secret = Some(orch8_types::config::SecretString::new(enc));
        }
        Ok(t)
    }

    /// Decrypt `TriggerDef.secret` after read (no-op if it was stored before
    /// encryption was enabled).
    fn decrypt_trigger(
        &self,
        trigger: &mut orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        if let Some(secret) = trigger.secret.as_ref() {
            let dec = self.decrypt_string_field(secret.expose())?;
            trigger.secret = Some(orch8_types::config::SecretString::new(dec));
        }
        Ok(())
    }

    /// Encrypt a plaintext string field for at-rest storage. No-op (returns the
    /// input) if it is already encrypted. Used for opaque secret-bearing string
    /// columns (trigger secrets, mobile-command payloads).
    fn encrypt_string_field(&self, plain: &str) -> Result<String, StorageError> {
        let val = serde_json::Value::String(plain.to_string());
        if orch8_types::encryption::FieldEncryptor::is_encrypted(&val) {
            return Ok(plain.to_string());
        }
        match self
            .encryptor
            .encrypt_value(&val)
            .map_err(|e| StorageError::Query(format!("field encryption: {e}")))?
        {
            serde_json::Value::String(s) => Ok(s),
            _ => Ok(plain.to_string()),
        }
    }

    /// Decrypt a string field encrypted by [`Self::encrypt_string_field`].
    /// Returns the input unchanged if it was not encrypted (blobs written
    /// before encryption was enabled stay readable).
    fn decrypt_string_field(&self, stored: &str) -> Result<String, StorageError> {
        let val = serde_json::Value::String(stored.to_string());
        if !orch8_types::encryption::FieldEncryptor::is_encrypted(&val) {
            return Ok(stored.to_string());
        }
        match self
            .encryptor
            .decrypt_value(&val)
            .map_err(|e| StorageError::Query(format!("field decryption: {e}")))?
        {
            serde_json::Value::String(s) => Ok(s),
            _ => Ok(stored.to_string()),
        }
    }
}

// ============================================================================
// Sub-trait 1: SequenceStore -- pure pass-through
// ============================================================================

passthrough_impl! {
    impl crate::SequenceStore for EncryptingStorage {
        async fn create_sequence(&self, seq: &orch8_types::sequence::SequenceDefinition) -> Result<(), StorageError>;
        async fn get_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn get_sequences(&self, ids: &[orch8_types::ids::SequenceId]) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn get_sequence_by_name(&self, tenant_id: &orch8_types::ids::TenantId, namespace: &orch8_types::ids::Namespace, name: &str, version: Option<i32>) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn list_sequence_versions(&self, tenant_id: &orch8_types::ids::TenantId, namespace: &orch8_types::ids::Namespace, name: &str) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn list_sequences(&self, tenant_id: Option<&orch8_types::ids::TenantId>, namespace: Option<&orch8_types::ids::Namespace>, limit: u32, offset: u32) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn deprecate_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<(), StorageError>;
        async fn update_sequence_status(&self, id: orch8_types::ids::SequenceId, status: &str) -> Result<(), StorageError>;
        async fn delete_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<(), StorageError>;
        // Must delegate explicitly -- without this override, the trait default
        // (delete then create, two separate writes) shadows the inner
        // backend's atomic single-transaction replace whenever encryption is
        // enabled. Still a pure pass-through, just one that must be listed.
        async fn replace_sequence(&self, old_id: orch8_types::ids::SequenceId, new: &orch8_types::sequence::SequenceDefinition) -> Result<(), StorageError>;
        async fn acquire_manifest_lock(&self, tenant_id: &str) -> Result<crate::ManifestLockGuard, StorageError>;
        async fn create_release(&self, release: &orch8_types::release::WorkflowRelease) -> Result<(), StorageError>;
        async fn get_release(&self, id: Uuid) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError>;
        async fn list_releases(&self, tenant_id: Option<&orch8_types::ids::TenantId>, limit: u32) -> Result<Vec<orch8_types::release::WorkflowRelease>, StorageError>;
        async fn cas_release_state(&self, id: Uuid, expected: orch8_types::release::ReleaseState, next: orch8_types::release::ReleaseState, canary_percent: Option<u8>, canary_started_at: Option<chrono::DateTime<chrono::Utc>>) -> Result<bool, StorageError>;
        async fn set_release_validation_summary(&self, id: Uuid, summary: &serde_json::Value) -> Result<(), StorageError>;
        async fn record_release_decision(&self, decision: &orch8_types::release::ReleaseDecision) -> Result<(), StorageError>;
        async fn list_release_decisions(&self, release_id: Uuid) -> Result<Vec<orch8_types::release::ReleaseDecision>, StorageError>;
        async fn find_routing_release_for_sequence(&self, baseline_sequence_id: orch8_types::ids::SequenceId) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError>;
    }
}

passthrough_impl! {
    impl crate::ContinuityStore for EncryptingStorage {
        async fn create_continuity_execution(&self, execution: &orch8_types::continuity::ContinuityExecution) -> Result<(), StorageError>;
        async fn get_continuity_execution(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::ContinuityId) -> Result<Option<orch8_types::continuity::ContinuityExecution>, StorageError>;
        async fn cas_continuity_owner(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::ContinuityId, expected_epoch: orch8_types::continuity::ExecutionEpoch, expected_owner: orch8_types::continuity::RuntimeId, next: &orch8_types::continuity::ContinuityExecution) -> Result<bool, StorageError>;
        async fn create_handoff(&self, handoff: &orch8_types::continuity::ExecutionHandoff) -> Result<(), StorageError>;
        async fn get_handoff(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::HandoffId) -> Result<Option<orch8_types::continuity::ExecutionHandoff>, StorageError>;
        async fn cas_handoff(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::HandoffId, expected_state: orch8_types::continuity::HandoffState, expected_version: u64, next: &orch8_types::continuity::ExecutionHandoff) -> Result<bool, StorageError>;
        async fn accept_handoff(&self, tenant_id: &orch8_types::ids::TenantId, expected_handoff: &orch8_types::continuity::ExecutionHandoff, accepted_handoff: &orch8_types::continuity::ExecutionHandoff, expected_execution: &orch8_types::continuity::ContinuityExecution, accepted_execution: &orch8_types::continuity::ContinuityExecution) -> Result<bool, StorageError>;
        async fn commit_handoff_export(&self, tenant_id: &orch8_types::ids::TenantId, expected_handoff: &orch8_types::continuity::ExecutionHandoff, exported_handoff: &orch8_types::continuity::ExecutionHandoff, expected_execution: &orch8_types::continuity::ContinuityExecution, transferring_execution: &orch8_types::continuity::ContinuityExecution) -> Result<bool, StorageError>;
        async fn resume_handoff(&self, tenant_id: &orch8_types::ids::TenantId, expected_handoff: &orch8_types::continuity::ExecutionHandoff, resumed_handoff: &orch8_types::continuity::ExecutionHandoff, destination_instance_id: orch8_types::ids::InstanceId) -> Result<bool, StorageError>;
        async fn save_capsule_manifest(&self, manifest: &orch8_types::continuity::CapsuleManifest) -> Result<(), StorageError>;
        async fn get_capsule_manifest(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::CapsuleId) -> Result<Option<orch8_types::continuity::CapsuleManifest>, StorageError>;
        async fn import_capsule_instance(&self, capsule_id: orch8_types::continuity::CapsuleId, destination_runtime_id: orch8_types::continuity::RuntimeId, instance: &orch8_types::instance::TaskInstance, checkpoint: &orch8_types::checkpoint::Checkpoint) -> Result<orch8_types::ids::InstanceId, StorageError> {
            let encrypted_instance = self.encrypt_instance(instance)?;
            let mut encrypted_checkpoint = checkpoint.clone();
            if !FieldEncryptor::is_encrypted(&encrypted_checkpoint.checkpoint_data) {
                encrypted_checkpoint.checkpoint_data = self.encrypt_json_value(&encrypted_checkpoint.checkpoint_data)?;
            }
            self.inner.import_capsule_instance(capsule_id, destination_runtime_id, encrypted_instance.as_ref(), &encrypted_checkpoint).await
        }
        async fn upsert_runtime_capabilities(&self, tenant_id: &orch8_types::ids::TenantId, capabilities: &orch8_types::continuity::RuntimeCapabilities) -> Result<(), StorageError>;
        async fn list_runtime_capabilities(&self, tenant_id: &orch8_types::ids::TenantId, observed_after: chrono::DateTime<chrono::Utc>, limit: u32) -> Result<Vec<orch8_types::continuity::RuntimeCapabilities>, StorageError>;
        async fn create_effect_receipt(&self, receipt: &orch8_types::continuity::EffectReceipt) -> Result<(), StorageError>;
        async fn get_effect_receipt(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::EffectId) -> Result<Option<orch8_types::continuity::EffectReceipt>, StorageError>;
        async fn cas_effect_receipt(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::EffectId, expected_state: orch8_types::continuity::EffectState, next: &orch8_types::continuity::EffectReceipt) -> Result<bool, StorageError>;
        async fn list_effect_receipts(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId, limit: u32) -> Result<Vec<orch8_types::continuity::EffectReceipt>, StorageError>;
        async fn append_provenance(&self, entry: &orch8_types::continuity::ProvenanceEntry) -> Result<(), StorageError>;
        async fn list_provenance(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId, limit: u32) -> Result<Vec<orch8_types::continuity::ProvenanceEntry>, StorageError>;
        async fn create_continuation_grant(&self, grant: &orch8_types::continuity::ContinuationGrant) -> Result<(), StorageError>;
        async fn get_continuation_grant(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::ContinuationGrantId) -> Result<Option<orch8_types::continuity::ContinuationGrant>, StorageError>;
        async fn consume_continuation_grant(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::ContinuationGrantId, nonce_sha256: &str, now: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn cas_continuation_grant_state(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::ContinuationGrantId, expected: orch8_types::continuity::ContinuationGrantState, next: &orch8_types::continuity::ContinuationGrant) -> Result<bool, StorageError>;
        async fn save_placement_decision(&self, decision: &orch8_types::continuity::PlacementDecision) -> Result<(), StorageError>;
        async fn get_placement_decision(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity::PlacementDecisionId) -> Result<Option<orch8_types::continuity::PlacementDecision>, StorageError>;
        async fn create_continuity_stream(&self, stream: &orch8_types::continuity::ContinuityStream) -> Result<(), StorageError>;
        async fn get_continuity_stream(&self, tenant_id: &orch8_types::ids::TenantId, stream_id: orch8_types::continuity::StreamId) -> Result<Option<orch8_types::continuity::ContinuityStream>, StorageError>;
        async fn append_stream_frame(&self, frame: &orch8_types::continuity::StreamFrame) -> Result<bool, StorageError>;
        async fn list_stream_frames(&self, tenant_id: &orch8_types::ids::TenantId, stream_id: orch8_types::continuity::StreamId, after_sequence: Option<u64>, now: chrono::DateTime<chrono::Utc>, limit: u32) -> Result<Vec<orch8_types::continuity::StreamFrame>, StorageError>;
        async fn retract_stream_frames(&self, tenant_id: &orch8_types::ids::TenantId, stream_id: orch8_types::continuity::StreamId, epoch: orch8_types::continuity::ExecutionEpoch, after_sequence: u64) -> Result<u64, StorageError>;
    }
}

passthrough_impl! {
    impl crate::InvariantStore for EncryptingStorage {
        async fn create_workflow_invariant(&self, invariant: &orch8_types::continuity_advanced::WorkflowInvariant) -> Result<(), StorageError>;
        async fn list_workflow_invariants(&self, tenant_id: &orch8_types::ids::TenantId, sequence_id: orch8_types::ids::SequenceId, sequence_version: i32, limit: u32) -> Result<Vec<orch8_types::continuity_advanced::WorkflowInvariant>, StorageError>;
        async fn append_invariant_result(&self, tenant_id: &orch8_types::ids::TenantId, result: &orch8_types::continuity_advanced::InvariantResult) -> Result<bool, StorageError>;
        async fn list_invariant_results(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId, limit: u32) -> Result<Vec<orch8_types::continuity_advanced::InvariantResult>, StorageError>;
    }
}

passthrough_impl! {
    impl crate::EvaluationStore for EncryptingStorage {
        async fn append_evaluation_score(&self, score: &orch8_types::continuity_advanced::EvaluationScore) -> Result<bool, StorageError>;
        async fn list_evaluation_scores(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId, limit: u32) -> Result<Vec<orch8_types::continuity_advanced::EvaluationScore>, StorageError>;
    }
}

passthrough_impl! {
    impl crate::AttentionStore for EncryptingStorage {
        async fn create_attention_task(&self, task: &orch8_types::continuity_advanced::AttentionTask) -> Result<(), StorageError>;
        async fn get_attention_task(&self, tenant_id: &orch8_types::ids::TenantId, id: orch8_types::continuity_advanced::AttentionTaskId) -> Result<Option<orch8_types::continuity_advanced::AttentionTask>, StorageError>;
        async fn claim_attention_task(&self, tenant_id: &orch8_types::ids::TenantId, expected: &orch8_types::continuity_advanced::AttentionTask, assigned: &orch8_types::continuity_advanced::AttentionTask, now: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn reserve_budget(&self, reservation: &orch8_types::continuity_advanced::BudgetReservation, budget: &orch8_types::instance::Budget) -> Result<bool, StorageError>;
    }
}

// ============================================================================
// Sub-trait 2: InstanceStore -- encryption on create/get/update context paths
// ============================================================================

passthrough_impl! {
    impl crate::InstanceStore for EncryptingStorage {
    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError> {
        let encrypted = self.encrypt_instance(instance)?;
        self.inner.create_instance(encrypted.as_ref()).await
    }

    async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        if instances
            .iter()
            .all(|i| FieldEncryptor::is_encrypted(&i.context.data))
        {
            return self.inner.create_instances_batch(instances).await;
        }
        let encrypted: Vec<TaskInstance> = instances
            .iter()
            .map(|i| self.encrypt_instance(i).map(Cow::into_owned))
            .collect::<Result<_, _>>()?;
        self.inner.create_instances_batch(&encrypted).await
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        let mut inst = self.inner.get_instance(id).await?;
        if let Some(ref mut i) = inst {
            self.decrypt_instance(i)?;
        }
        Ok(inst)
    }

    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        max_per_tenant: u32,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self
            .inner
            .claim_due_instances(now, limit, max_per_tenant)
            .await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_context(id, context)?;
        self.inner
            .update_instance_context(id, encrypted.as_ref())
            .await
    }

    async fn update_instance_context_cas(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let encrypted = self.encrypt_context(id, context)?;
        self.inner
            .update_instance_context_cas(id, encrypted.as_ref(), expected_updated_at)
            .await
    }

    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        const MAX_ATTEMPTS: u32 = 20;
        const BASE_BACKOFF_MS: u64 = 5;
        const MAX_BACKOFF_MS: u64 = 200;

        // Pre-allocate the key string outside the CAS loop to avoid
        // redundant heap allocations on each retry.
        let key_owned = key.to_string();

        // Encrypted context is a single blob -- read -> decrypt -> merge ->
        // encrypt -> CAS write. Retry on contention (another writer
        // updated `updated_at` between our read and write). Each retry
        // re-decrypts/re-encrypts the whole blob, so under sustained
        // concurrent writers to the same instance this is markedly more
        // contention-prone than the plaintext path's single atomic
        // `jsonb_set`; jittered backoff plus a generous retry bound keeps a
        // burst of concurrent branch/signal writers from hard-failing the
        // step instead of just taking a little longer.
        //
        // MAX_ATTEMPTS=20 is an intentional fan-in ceiling, not a bug to be
        // "fixed" into an unbounded loop: it bounds worst-case latency for a
        // single `merge_context_data` call under contention. If a workload
        // regularly needs more than ~20 concurrent writers merging into the
        // same instance's context, that's a signal to raise the bound (and
        // re-check `MAX_BACKOFF_MS`) rather than remove it.
        for attempt in 0..MAX_ATTEMPTS {
            let Some(mut instance) = self.inner.get_instance(id).await? else {
                return Ok(());
            };
            let snapshot_ts = instance.updated_at;
            self.decrypt_instance(&mut instance)?;

            if !instance.context.data.is_object() {
                instance.context.data = serde_json::Value::Object(serde_json::Map::new());
            }
            if let Some(map) = instance.context.data.as_object_mut() {
                map.insert(key_owned.clone(), value.clone());
            }

            self.encrypt_context_mut(id, &mut instance.context)?;
            if self
                .inner
                .update_instance_context_cas(id, &instance.context, snapshot_ts)
                .await?
            {
                return Ok(());
            }

            let backoff_ms =
                (BASE_BACKOFF_MS.saturating_mul(1u64 << attempt.min(6))).min(MAX_BACKOFF_MS);
            let jittered_ms = rand::random_range(backoff_ms / 2..=backoff_ms.max(1));
            tokio::time::sleep(std::time::Duration::from_millis(jittered_ms)).await;
        }
        Err(StorageError::Query(format!(
            "merge_context_data: CAS contention exceeded retries ({MAX_ATTEMPTS}) for instance {id}"
        )))
    }

    // Metadata is never encrypted (only `context.data` is) — pure pass-through.
    async fn merge_instance_metadata(&self, id: InstanceId, patch: &serde_json::Value) -> Result<(), StorageError>;

    async fn list_instances(
        &self,
        filter: &orch8_types::filter::InstanceFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self.inner.list_instances(filter, pagination).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    async fn list_waiting_with_trees(
        &self,
        filter: &orch8_types::filter::InstanceFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<(TaskInstance, Vec<orch8_types::execution::ExecutionNode>)>, StorageError> {
        let mut pairs = self
            .inner
            .list_waiting_with_trees(filter, pagination)
            .await?;
        for (inst, _) in &mut pairs {
            self.decrypt_instance(inst)?;
        }
        Ok(pairs)
    }

    async fn find_by_idempotency_key(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        let mut inst = self
            .inner
            .find_by_idempotency_key(tenant_id, idempotency_key)
            .await?;
        if let Some(ref mut i) = inst {
            self.decrypt_instance(i)?;
        }
        Ok(inst)
    }

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self.inner.get_child_instances(parent_instance_id).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    // `create_instance_externalized` / `create_instances_batch_externalized` /
    // `update_instance_context_externalized` must externalize *before*
    // encrypting: `externalize_fields` requires `context.data` to still be a
    // JSON object, but whole-blob encryption collapses it to a single string,
    // so encrypting first silently disables externalization (oversized
    // contexts land inline, TOAST-bloating `task_instances` and starving the
    // GC/size-cap machinery). Doing it in this order also means the
    // externalized-state payloads pulled out here get encrypted individually
    // via `batch_save_externalized_state` below, rather than left in plaintext.
    //
    // Trade-off: the inner backend's `create_instance_externalized` commits
    // the instance row and its externalized-state rows in one transaction;
    // externalizing here means the wrapper issues two separate writes, the
    // same non-atomic shape the default trait impl in `lib.rs` already
    // documents as the accepted fallback for backends that don't implement
    // it natively. Unlike that default, the instance row is written *first*:
    // `externalized_state.instance_id` has a `FOREIGN KEY ... ON DELETE
    // CASCADE` (SQLite has `PRAGMA foreign_keys = ON`), so an externalized
    // row referencing an instance that doesn't exist yet is rejected outright.
    async fn create_instance_externalized(
        &self,
        instance: &TaskInstance,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        if FieldEncryptor::is_encrypted(&instance.context.data) {
            // Nothing left to externalize -- pass through untouched.
            return self
                .inner
                .create_instance_externalized(instance, threshold_bytes)
                .await;
        }
        let mut inst_clone = instance.clone();
        let refs = crate::externalizing::externalize_fields(
            &mut inst_clone.context.data,
            &instance.id.into_uuid().to_string(),
            threshold_bytes,
        );
        self.encrypt_context_mut(instance.id, &mut inst_clone.context)?;
        self.inner.create_instance(&inst_clone).await?;
        if !refs.is_empty() {
            crate::InstanceStore::batch_save_externalized_state(self, instance.id, &refs).await?;
        }
        Ok(())
    }

    async fn create_instances_batch_externalized(
        &self,
        instances: &[TaskInstance],
        threshold_bytes: u32,
    ) -> Result<u64, StorageError> {
        if instances
            .iter()
            .all(|i| FieldEncryptor::is_encrypted(&i.context.data))
        {
            return self
                .inner
                .create_instances_batch_externalized(instances, threshold_bytes)
                .await;
        }
        let mut clones: Vec<TaskInstance> = Vec::with_capacity(instances.len());
        let mut all_refs: Vec<(InstanceId, Vec<(String, serde_json::Value)>)> = Vec::new();
        for inst in instances {
            if FieldEncryptor::is_encrypted(&inst.context.data) {
                clones.push(inst.clone());
                continue;
            }
            let mut c = inst.clone();
            let refs = crate::externalizing::externalize_fields(
                &mut c.context.data,
                &inst.id.into_uuid().to_string(),
                threshold_bytes,
            );
            self.encrypt_context_mut(inst.id, &mut c.context)?;
            if !refs.is_empty() {
                all_refs.push((inst.id, refs));
            }
            clones.push(c);
        }
        // Instance rows must land before externalized-state rows (FK on
        // `externalized_state.instance_id`), so persist the batch first and
        // the refs after -- same ordering constraint as
        // `create_instance_externalized`.
        let count = self.inner.create_instances_batch(&clones).await?;
        for (instance_id, refs) in &all_refs {
            crate::InstanceStore::batch_save_externalized_state(self, *instance_id, refs).await?;
        }
        Ok(count)
    }

    async fn update_instance_context_externalized(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        if FieldEncryptor::is_encrypted(&context.data) {
            return self
                .inner
                .update_instance_context_externalized(id, context, threshold_bytes)
                .await;
        }
        let mut ctx_clone = context.clone();
        let refs = crate::externalizing::externalize_fields(
            &mut ctx_clone.data,
            &id.into_uuid().to_string(),
            threshold_bytes,
        );
        self.encrypt_context_mut(id, &mut ctx_clone)?;
        if !refs.is_empty() {
            crate::InstanceStore::batch_save_externalized_state(self, id, &refs).await?;
        }
        self.inner.update_instance_context(id, &ctx_clone).await
    }

    async fn create_instance_with_dedupe(
        &self,
        scope: &crate::DedupeScope,
        key: &str,
        instance: &TaskInstance,
    ) -> Result<crate::EmitDedupeOutcome, StorageError> {
        let encrypted = self.encrypt_instance(instance)?;
        self.inner
            .create_instance_with_dedupe(scope, key, encrypted.as_ref())
            .await
    }

    // --- Pass-through InstanceStore methods ---

    async fn update_instance_state(&self, id: InstanceId, new_state: orch8_types::instance::InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
    async fn batch_reschedule_instances(&self, ids: &[InstanceId], fire_at: DateTime<Utc>) -> Result<(), StorageError>;
    async fn conditional_update_instance_state(&self, id: InstanceId, expected_state: orch8_types::instance::InstanceState, new_state: orch8_types::instance::InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
    async fn update_instance_sequence(&self, id: InstanceId, new_sequence_id: orch8_types::ids::SequenceId) -> Result<(), StorageError>;
    async fn count_instances(&self, filter: &orch8_types::filter::InstanceFilter) -> Result<u64, StorageError>;
    async fn bulk_update_state(&self, filter: &orch8_types::filter::InstanceFilter, new_state: orch8_types::instance::InstanceState) -> Result<u64, StorageError>;
    async fn bulk_reschedule(&self, filter: &orch8_types::filter::InstanceFilter, offset_secs: i64) -> Result<u64, StorageError>;
    async fn update_instance_started_at(&self, id: InstanceId, started_at: DateTime<Utc>) -> Result<(), StorageError>;
    // Pass-through: the counter lives in `context.runtime`, which is never
    // encrypted (only `context.data` is), so the inner backend's atomic
    // increment is correct as-is.
    async fn increment_total_steps(&self, id: InstanceId) -> Result<u32, StorageError>;
    async fn update_instance_current_step_started_at(&self, id: InstanceId, ts: DateTime<Utc>) -> Result<(), StorageError>;
    async fn count_running_by_concurrency_key(&self, concurrency_key: &str) -> Result<i64, StorageError>;
    async fn count_running_by_concurrency_keys(&self, concurrency_keys: &[&str]) -> Result<std::collections::HashMap<String, i64>, StorageError>;
    async fn concurrency_position(&self, instance_id: InstanceId, concurrency_key: &str) -> Result<i64, StorageError>;
    async fn recover_stale_instances(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;
    async fn heartbeat_instance(&self, instance_id: InstanceId) -> Result<(), StorageError>;
    async fn inject_blocks(&self, instance_id: InstanceId, blocks_json: &serde_json::Value) -> Result<(), StorageError>;
    async fn inject_blocks_at_position(&self, instance_id: InstanceId, new_blocks_json: &serde_json::Value, position: Option<usize>) -> Result<serde_json::Value, StorageError>;
    async fn get_injected_blocks(&self, instance_id: InstanceId) -> Result<Option<serde_json::Value>, StorageError>;
    async fn record_or_get_emit_dedupe(&self, scope: &crate::DedupeScope, key: &str, candidate_child: InstanceId) -> Result<crate::EmitDedupeOutcome, StorageError>;
    async fn delete_expired_emit_event_dedupe(&self, older_than: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;

    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        let encrypted: Vec<(String, serde_json::Value)> = entries
            .iter()
            .map(|(k, v)| self.encrypt_json_value(v).map(|ev| (k.clone(), ev)))
            .collect::<Result<_, _>>()?;
        crate::InstanceStore::batch_save_externalized_state(&*self.inner, instance_id, &encrypted)
            .await
    }
    }
}

// ============================================================================
// Sub-trait 3: ExecutionTreeStore -- pure pass-through
// ============================================================================

passthrough_impl! {
    impl crate::ExecutionTreeStore for EncryptingStorage {
        async fn create_execution_node(&self, node: &orch8_types::execution::ExecutionNode) -> Result<(), StorageError>;
        async fn create_execution_nodes_batch(&self, nodes: &[orch8_types::execution::ExecutionNode]) -> Result<(), StorageError>;
        async fn get_execution_tree(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError>;
        async fn update_node_state(&self, node_id: orch8_types::ids::ExecutionNodeId, state: orch8_types::execution::NodeState) -> Result<(), StorageError>;
        async fn batch_activate_nodes(&self, node_ids: &[orch8_types::ids::ExecutionNodeId]) -> Result<(), StorageError>;
        async fn update_nodes_state(&self, node_ids: &[orch8_types::ids::ExecutionNodeId], state: orch8_types::execution::NodeState) -> Result<(), StorageError>;
        async fn get_children(&self, parent_id: orch8_types::ids::ExecutionNodeId) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError>;
        async fn delete_execution_tree(&self, instance_id: InstanceId) -> Result<(), StorageError>;
    }
}

// ============================================================================
// Sub-trait 4: OutputStore -- pass-through (context encryption handled via
// InstanceStore methods that OutputStore calls reference)
// ============================================================================

passthrough_impl! {
    impl crate::OutputStore for EncryptingStorage {
    async fn save_block_output(
        &self,
        output: &orch8_types::output::BlockOutput,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_block_output(output)?;
        self.inner.save_block_output(encrypted.as_ref()).await
    }
    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &orch8_types::ids::BlockId,
    ) -> Result<Option<orch8_types::output::BlockOutput>, StorageError> {
        let mut out = self.inner.get_block_output(instance_id, block_id).await?;
        if let Some(ref mut o) = out {
            self.decrypt_block_output(o)?;
        }
        Ok(out)
    }
    async fn get_block_outputs_batch(
        &self,
        keys: &[(InstanceId, &orch8_types::ids::BlockId)],
    ) -> Result<
        std::collections::HashMap<
            (InstanceId, orch8_types::ids::BlockId),
            orch8_types::output::BlockOutput,
        >,
        StorageError,
    > {
        let mut out = self.inner.get_block_outputs_batch(keys).await?;
        for o in out.values_mut() {
            self.decrypt_block_output(o)?;
        }
        Ok(out)
    }
    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError> {
        let mut out = self.inner.get_all_outputs(instance_id).await?;
        for o in &mut out {
            self.decrypt_block_output(o)?;
        }
        Ok(out)
    }
    async fn get_outputs_after_created_at(
        &self,
        instance_id: InstanceId,
        after: Option<DateTime<Utc>>,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError> {
        let mut out = self
            .inner
            .get_outputs_after_created_at(instance_id, after)
            .await?;
        for o in &mut out {
            self.decrypt_block_output(o)?;
        }
        Ok(out)
    }
    async fn get_completed_block_ids(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::ids::BlockId>, StorageError>;
    async fn get_completed_block_ids_batch(&self, instance_ids: &[InstanceId]) -> Result<std::collections::HashMap<InstanceId, Vec<orch8_types::ids::BlockId>>, StorageError>;

    async fn save_output_and_transition(
        &self,
        output: &orch8_types::output::BlockOutput,
        instance_id: InstanceId,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let encrypted_output = self.encrypt_block_output(output)?;
        self.inner
            .save_output_and_transition(
                encrypted_output.as_ref(),
                instance_id,
                new_state,
                next_fire_at,
            )
            .await
    }
    async fn save_output_merge_context_and_transition(
        &self,
        output: &orch8_types::output::BlockOutput,
        instance_id: InstanceId,
        context: &ExecutionContext,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let encrypted_output = self.encrypt_block_output(output)?;
        let encrypted = self.encrypt_context(instance_id, context)?;
        self.inner
            .save_output_merge_context_and_transition(
                encrypted_output.as_ref(),
                instance_id,
                encrypted.as_ref(),
                new_state,
                next_fire_at,
            )
            .await
    }
    async fn save_output_complete_node_and_transition(
        &self,
        output: &orch8_types::output::BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        instance_id: InstanceId,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let encrypted_output = self.encrypt_block_output(output)?;
        self.inner
            .save_output_complete_node_and_transition(
                encrypted_output.as_ref(),
                node_id,
                instance_id,
                new_state,
                next_fire_at,
            )
            .await
    }
    async fn save_output_complete_node_merge_context_and_transition(
        &self,
        output: &orch8_types::output::BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        instance_id: InstanceId,
        context: &ExecutionContext,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let encrypted_output = self.encrypt_block_output(output)?;
        let encrypted = self.encrypt_context(instance_id, context)?;
        self.inner
            .save_output_complete_node_merge_context_and_transition(
                encrypted_output.as_ref(),
                node_id,
                instance_id,
                encrypted.as_ref(),
                new_state,
                next_fire_at,
            )
            .await
    }
    async fn delete_block_outputs(&self, instance_id: InstanceId, block_id: &orch8_types::ids::BlockId) -> Result<u64, StorageError>;
    async fn delete_block_outputs_batch(&self, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId]) -> Result<u64, StorageError>;
    async fn delete_all_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
    async fn delete_sentinel_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
    async fn delete_block_output_by_id(&self, id: Uuid) -> Result<(), StorageError>;

    async fn get_outputs_page(
        &self,
        instance_id: InstanceId,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError> {
        let mut out = self
            .inner
            .get_outputs_page(instance_id, limit, offset)
            .await?;
        for o in &mut out {
            self.decrypt_block_output(o)?;
        }
        Ok(out)
    }
    // Pass-through: this is a raw row copy on the inner backend, so a
    // ciphertext `output` column is copied verbatim -- consistent with what
    // `save_block_output` would have written for the destination instance.
    async fn copy_block_outputs(&self, src: InstanceId, dst: InstanceId, block_ids: &[orch8_types::ids::BlockId]) -> Result<u64, StorageError>;
    }
}

// ============================================================================
// Sub-trait 5: SignalStore -- pure pass-through
// ============================================================================

passthrough_impl! {
    impl crate::SignalStore for EncryptingStorage {
    async fn enqueue_signal(
        &self,
        signal: &orch8_types::signal::Signal,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_signal(signal)?;
        self.inner.enqueue_signal(encrypted.as_ref()).await
    }
    async fn enqueue_signal_if_active(
        &self,
        signal: &orch8_types::signal::Signal,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_signal(signal)?;
        self.inner
            .enqueue_signal_if_active(encrypted.as_ref())
            .await
    }
    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::signal::Signal>, StorageError> {
        let mut signals = self.inner.get_pending_signals(instance_id).await?;
        for s in &mut signals {
            self.decrypt_signal(s)?;
        }
        Ok(signals)
    }
    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<orch8_types::signal::Signal>>, StorageError>
    {
        let mut batch = self.inner.get_pending_signals_batch(instance_ids).await?;
        for signals in batch.values_mut() {
            for s in signals {
                self.decrypt_signal(s)?;
            }
        }
        Ok(batch)
    }
    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError>;
    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError>;
    async fn get_signalled_instance_ids(&self, limit: u32) -> Result<Vec<(InstanceId, orch8_types::instance::InstanceState)>, StorageError>;
    async fn ingest_event(&self, envelope: &orch8_types::event_correlation::EventEnvelope) -> Result<bool, StorageError>;
    async fn get_event(&self, id: Uuid) -> Result<Option<orch8_types::event_correlation::EventEnvelope>, StorageError>;
    async fn list_events(&self, tenant_id: &str, status: Option<orch8_types::event_correlation::EventStatus>, limit: u32) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;
    async fn find_pending_events(&self, tenant_id: &str, event_names: &[String], correlation_key: &str) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;
    async fn consume_events(&self, event_ids: &[Uuid], instance_id: InstanceId) -> Result<u64, StorageError>;
    async fn upsert_event_wait(&self, wait: &orch8_types::event_correlation::EventWait) -> Result<(), StorageError>;
    async fn get_event_wait(&self, instance_id: InstanceId, block_id: &str) -> Result<Option<orch8_types::event_correlation::EventWait>, StorageError>;
    async fn find_waiting_event_waits(&self, tenant_id: &str, event_name: &str, correlation_key: &str) -> Result<Vec<orch8_types::event_correlation::EventWait>, StorageError>;
    async fn update_event_wait(&self, wait: &orch8_types::event_correlation::EventWait, expected_status: orch8_types::event_correlation::WaitStatus) -> Result<bool, StorageError>;
    async fn expire_events_before(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<u64, StorageError>;
    }
}

// ============================================================================
// Sub-trait 6: WorkerStore -- pure pass-through
// ============================================================================

passthrough_impl! {
    impl crate::WorkerStore for EncryptingStorage {
    async fn create_worker_task(
        &self,
        task: &orch8_types::worker::WorkerTask,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_worker_task(task)?;
        self.inner.create_worker_task(encrypted.as_ref()).await
    }
    async fn get_worker_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<orch8_types::worker::WorkerTask>, StorageError> {
        let mut task = self.inner.get_worker_task(task_id).await?;
        if let Some(ref mut t) = task {
            self.decrypt_worker_task(t)?;
        }
        Ok(task)
    }
    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        let mut tasks = self
            .inner
            .claim_worker_tasks(handler_name, worker_id, limit)
            .await?;
        for t in &mut tasks {
            self.decrypt_worker_task(t)?;
        }
        Ok(tasks)
    }
    async fn claim_worker_tasks_for_tenant(
        &self,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        let mut tasks = self
            .inner
            .claim_worker_tasks_for_tenant(handler_name, worker_id, tenant_id, limit)
            .await?;
        for t in &mut tasks {
            self.decrypt_worker_task(t)?;
        }
        Ok(tasks)
    }
    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        let encrypted = self.encrypt_json_value(output)?;
        self.inner
            .complete_worker_task(task_id, worker_id, &encrypted)
            .await
    }
    async fn fail_worker_task(&self, task_id: Uuid, worker_id: &str, message: &str, retryable: bool) -> Result<bool, StorageError>;
    async fn heartbeat_worker_task(&self, task_id: Uuid, worker_id: &str) -> Result<bool, StorageError>;
    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError>;

    async fn retry_worker_task(
        &self,
        old_task_id: Uuid,
        new_task: &orch8_types::worker::WorkerTask,
        node_id: Option<orch8_types::ids::ExecutionNodeId>,
        instance_id: orch8_types::ids::InstanceId,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_worker_task(new_task)?;
        self.inner
            .retry_worker_task(
                old_task_id,
                encrypted.as_ref(),
                node_id,
                instance_id,
                fire_at,
            )
            .await
    }
    async fn reap_stale_worker_tasks(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;
    async fn expire_timed_out_worker_tasks(&self) -> Result<u64, StorageError>;
    async fn cancel_worker_tasks_for_blocks(&self, instance_id: Uuid, block_ids: &[String]) -> Result<u64, StorageError>;
    async fn cancel_worker_tasks_for_block(&self, instance_id: Uuid, block_id: &str) -> Result<u64, StorageError>;

    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        let mut tasks = self.inner.list_worker_tasks(filter, pagination).await?;
        for t in &mut tasks {
            self.decrypt_worker_task(t)?;
        }
        Ok(tasks)
    }
    async fn worker_task_stats(&self, tenant_id: Option<&orch8_types::ids::TenantId>) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError>;

    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        let mut tasks = self
            .inner
            .claim_worker_tasks_from_queue(queue_name, handler_name, worker_id, limit)
            .await?;
        for t in &mut tasks {
            self.decrypt_worker_task(t)?;
        }
        Ok(tasks)
    }
    async fn claim_worker_tasks_from_queue_for_tenant(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        let mut tasks = self
            .inner
            .claim_worker_tasks_from_queue_for_tenant(
                queue_name,
                handler_name,
                worker_id,
                tenant_id,
                limit,
            )
            .await?;
        for t in &mut tasks {
            self.decrypt_worker_task(t)?;
        }
        Ok(tasks)
    }

    async fn upsert_worker_registration(&self, registration: &orch8_types::worker::WorkerRegistration) -> Result<(), StorageError>;
    async fn list_worker_registrations(&self, seen_within_secs: Option<i64>) -> Result<Vec<orch8_types::worker::WorkerRegistration>, StorageError>;
    async fn claimed_task_counts_by_worker(&self) -> Result<Vec<(String, i64)>, StorageError>;
    async fn park_webhook(&self, entry: &orch8_types::webhook_outbox::WebhookOutboxEntry) -> Result<(), StorageError>;
    async fn list_webhook_outbox(&self, limit: u32) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;
    async fn get_webhook_outbox(&self, id: Uuid) -> Result<Option<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;
    async fn delete_webhook_outbox(&self, id: Uuid) -> Result<(), StorageError>;
    async fn record_webhook_attempt(&self, attempt: &orch8_types::webhook_delivery::WebhookDeliveryAttempt) -> Result<(), StorageError>;
    async fn list_webhook_deliveries(&self, filter: &orch8_types::webhook_delivery::DeliveryFilter, limit: u32) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliverySummary>, StorageError>;
    async fn get_webhook_delivery_attempts(&self, delivery_id: Uuid) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliveryAttempt>, StorageError>;
    async fn delete_webhook_attempts_before(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<u64, StorageError>;
    async fn create_queue_routing_rule(&self, rule: &orch8_types::queue_routing::QueueRoutingRule) -> Result<(), StorageError>;
    async fn list_queue_routing_rules(&self, tenant_id: Option<&orch8_types::ids::TenantId>, handler_name: Option<&str>) -> Result<Vec<orch8_types::queue_routing::QueueRoutingRule>, StorageError>;
    async fn get_queue_routing_rule(&self, id: Uuid) -> Result<Option<orch8_types::queue_routing::QueueRoutingRule>, StorageError>;
    async fn delete_queue_routing_rule(&self, id: Uuid) -> Result<(), StorageError>;
    async fn enqueue_worker_command(&self, command: &orch8_types::worker::WorkerCommand) -> Result<(), StorageError>;
    async fn list_worker_commands(&self, worker_id: &str) -> Result<Vec<orch8_types::worker::WorkerCommand>, StorageError>;
    async fn delete_worker_command(&self, id: Uuid) -> Result<(), StorageError>;
    async fn upsert_worker_version_pin(&self, pin: &orch8_types::worker::WorkerVersionPin) -> Result<(), StorageError>;
    async fn get_worker_version_pin(&self, tenant_id: &str, handler_name: &str) -> Result<Option<orch8_types::worker::WorkerVersionPin>, StorageError>;
    async fn list_worker_version_pins(&self, tenant_id: Option<&str>) -> Result<Vec<orch8_types::worker::WorkerVersionPin>, StorageError>;
    async fn delete_worker_version_pin(&self, tenant_id: &str, handler_name: &str) -> Result<(), StorageError>;
    async fn upsert_queue_dispatch(&self, config: &orch8_types::queue_dispatch::QueueDispatchConfig) -> Result<(), StorageError>;
    async fn get_queue_dispatch(&self, tenant_id: &str, queue_name: &str) -> Result<Option<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError>;
    async fn list_queue_dispatch(&self, tenant_id: Option<&str>) -> Result<Vec<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError>;
    async fn delete_queue_dispatch(&self, tenant_id: &str, queue_name: &str) -> Result<(), StorageError>;

    // Step log messages can echo step input/output (handler errors, template
    // debug output) -- the same data class as context, so the message text
    // is encrypted at rest like other secret-bearing string columns.
    async fn append_step_logs(
        &self,
        instance_id: orch8_types::ids::InstanceId,
        block_id: &orch8_types::ids::BlockId,
        entries: &[orch8_types::step_log::StepLogEntry],
    ) -> Result<(), StorageError> {
        let encrypted: Vec<orch8_types::step_log::StepLogEntry> = entries
            .iter()
            .map(|e| {
                Ok::<_, StorageError>(orch8_types::step_log::StepLogEntry {
                    ts: e.ts,
                    level: e.level.clone(),
                    message: self.encrypt_string_field(&e.message)?,
                })
            })
            .collect::<Result<_, _>>()?;
        self.inner
            .append_step_logs(instance_id, block_id, &encrypted)
            .await
    }

    async fn list_step_logs(
        &self,
        instance_id: orch8_types::ids::InstanceId,
    ) -> Result<Vec<orch8_types::step_log::StepLog>, StorageError> {
        let mut logs = self.inner.list_step_logs(instance_id).await?;
        for l in &mut logs {
            l.message = self.decrypt_string_field(&l.message)?;
        }
        Ok(logs)
    }
    }
}

// ============================================================================
// Sub-trait 7: SchedulingStore -- pure pass-through
// ============================================================================

passthrough_impl! {
    impl crate::SchedulingStore for EncryptingStorage {
        async fn create_cron_schedule(&self, schedule: &orch8_types::cron::CronSchedule) -> Result<(), StorageError>;
        async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<orch8_types::cron::CronSchedule>, StorageError>;
        async fn list_cron_schedules(&self, tenant_id: Option<&orch8_types::ids::TenantId>, limit: u32) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError>;
        async fn update_cron_schedule(&self, schedule: &orch8_types::cron::CronSchedule) -> Result<(), StorageError>;
        async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError>;
        async fn claim_due_cron_schedules(&self, now: DateTime<Utc>) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError>;
        async fn update_cron_fire_times(&self, id: Uuid, last_triggered_at: DateTime<Utc>, next_fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn record_cron_skip(&self, id: Uuid, now: DateTime<Utc>, next_fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn active_instance_ids_for_cron(&self, cron_id: Uuid, limit: u32) -> Result<Vec<orch8_types::ids::InstanceId>, StorageError>;
        async fn check_rate_limit(&self, tenant_id: &orch8_types::ids::TenantId, resource_key: &orch8_types::ids::ResourceKey, now: DateTime<Utc>) -> Result<orch8_types::rate_limit::RateLimitCheck, StorageError>;
        async fn upsert_rate_limit(&self, limit: &orch8_types::rate_limit::RateLimit) -> Result<(), StorageError>;
    }
}

// ============================================================================
// Sub-trait 8: AdminStore -- encryption on credential CRUD
// ============================================================================

passthrough_impl! {
    impl crate::AdminStore for EncryptingStorage {
    // --- Sessions ---
    async fn create_session(&self, session: &orch8_types::session::Session) -> Result<(), StorageError>;
    async fn get_session(&self, id: Uuid) -> Result<Option<orch8_types::session::Session>, StorageError>;
    async fn get_session_by_key(&self, tenant_id: &orch8_types::ids::TenantId, session_key: &str) -> Result<Option<orch8_types::session::Session>, StorageError>;
    async fn update_session_data(&self, id: Uuid, data: &serde_json::Value) -> Result<(), StorageError>;
    async fn update_session_state(&self, id: Uuid, state: orch8_types::session::SessionState) -> Result<(), StorageError>;

    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        // Note: InstanceStore impl already decrypts; but AdminStore also
        // declares this method. We delegate to inner and decrypt.
        let mut instances = self.inner.list_session_instances(session_id).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    // --- Plugins (pass-through) ---
    async fn create_plugin(&self, plugin: &orch8_types::plugin::PluginDef) -> Result<(), StorageError>;
    async fn get_plugin(&self, tenant_id: Option<&orch8_types::ids::TenantId>, name: &str) -> Result<Option<orch8_types::plugin::PluginDef>, StorageError>;
    async fn list_plugins(&self, tenant_id: Option<&orch8_types::ids::TenantId>) -> Result<Vec<orch8_types::plugin::PluginDef>, StorageError>;
    async fn update_plugin(&self, plugin: &orch8_types::plugin::PluginDef) -> Result<(), StorageError>;
    async fn delete_plugin(&self, name: &str) -> Result<(), StorageError>;

    // --- Triggers (secret encrypted at rest) ---
    // `TriggerDef.secret` is the HMAC key used to authenticate inbound webhook
    // payloads; persisting it in plaintext means a DB leak lets an attacker
    // forge signed webhooks. Encrypt on write, decrypt on read — same shape as
    // credentials. Other fields are pass-through.
    async fn create_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        self.inner
            .create_trigger(&self.encrypt_trigger(trigger)?)
            .await
    }
    async fn get_trigger(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        slug: &str,
    ) -> Result<Option<orch8_types::trigger::TriggerDef>, StorageError> {
        let mut trigger = self.inner.get_trigger(tenant_id, slug).await?;
        if let Some(t) = trigger.as_mut() {
            self.decrypt_trigger(t)?;
        }
        Ok(trigger)
    }
    async fn list_triggers(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError> {
        let mut triggers = self.inner.list_triggers(tenant_id, limit).await?;
        for t in &mut triggers {
            self.decrypt_trigger(t)?;
        }
        Ok(triggers)
    }
    async fn update_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        self.inner
            .update_trigger(&self.encrypt_trigger(trigger)?)
            .await
    }
    async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError>;
    async fn claim_webhook_nonce(&self, slug: &str, nonce: &str, expires_at: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
    async fn get_trigger_poll_state(&self, slug: &str) -> Result<Option<orch8_types::trigger::TriggerPollState>, StorageError>;
    async fn upsert_trigger_poll_state(&self, state: &orch8_types::trigger::TriggerPollState) -> Result<(), StorageError>;

    // --- Credentials (with encryption) ---
    async fn create_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_credential(credential)?;
        self.inner.create_credential(&encrypted).await
    }

    async fn get_credential(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        id: &str,
    ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError> {
        let mut cred = self.inner.get_credential(tenant_id, id).await?;
        if let Some(ref mut c) = cred {
            self.decrypt_credential(c)?;
        }
        Ok(cred)
    }

    async fn list_credentials(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        let mut creds = self.inner.list_credentials(tenant_id, limit).await?;
        for c in &mut creds {
            self.decrypt_credential(c)?;
        }
        Ok(creds)
    }

    async fn update_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_credential(credential)?;
        self.inner.update_credential(&encrypted).await
    }

    async fn delete_credential(&self, id: &str) -> Result<(), StorageError>;

    async fn list_credentials_due_for_refresh(
        &self,
        threshold: std::time::Duration,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        let mut creds = self
            .inner
            .list_credentials_due_for_refresh(threshold)
            .await?;
        for c in &mut creds {
            self.decrypt_credential(c)?;
        }
        Ok(creds)
    }

    // --- API keys (pass-through: records hold only a SHA-256 hash, no secret) ---
    async fn create_api_key(&self, key: &orch8_types::api_key::ApiKeyRecord) -> Result<(), StorageError>;
    async fn lookup_api_key_by_hash(&self, key_hash: &str) -> Result<Option<orch8_types::api_key::ApiKeyRecord>, StorageError>;
    async fn list_api_keys(&self, tenant_id: &orch8_types::ids::TenantId) -> Result<Vec<orch8_types::api_key::ApiKeyRecord>, StorageError>;
    async fn revoke_api_key(&self, id: &str) -> Result<bool, StorageError>;
    async fn touch_api_key(&self, id: &str, at: chrono::DateTime<chrono::Utc>) -> Result<(), StorageError>;

    // --- Cluster (pass-through) ---
    async fn register_node(&self, node: &orch8_types::cluster::ClusterNode) -> Result<(), StorageError>;
    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError>;
    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError>;
    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError>;
    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError>;
    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError>;
    async fn reap_stale_nodes(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;

    // --- Circuit Breakers (pass-through) ---
    async fn upsert_circuit_breaker(&self, state: &orch8_types::circuit_breaker::CircuitBreakerState) -> Result<(), StorageError>;
    async fn list_open_circuit_breakers(&self) -> Result<Vec<orch8_types::circuit_breaker::CircuitBreakerState>, StorageError>;
    async fn delete_circuit_breaker(&self, tenant_id: &orch8_types::ids::TenantId, handler: &str) -> Result<(), StorageError>;

    // --- Audit Log (pass-through) ---
    async fn append_audit_log(&self, entry: &orch8_types::audit::AuditLogEntry) -> Result<(), StorageError>;
    async fn list_audit_log(&self, instance_id: InstanceId, limit: u32) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError>;
    async fn list_audit_log_by_tenant(&self, tenant_id: &orch8_types::ids::TenantId, limit: u32) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError>;

    // --- Rollback policies (pass-through) ---
    async fn create_rollback_policy(&self, tenant_id: &str, sequence_name: &str, error_rate_threshold: f64, time_window_secs: i32, cooldown_secs: Option<i32>, confirmation_window_secs: Option<i32>, webhook_url: Option<&str>) -> Result<(), StorageError>;
    async fn get_rollback_policy(&self, tenant_id: &str, sequence_name: &str) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError>;
    async fn list_rollback_policies(&self, tenant_id: Option<&str>, limit: u32) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError>;
    async fn delete_rollback_policy(&self, tenant_id: &str, sequence_name: &str) -> Result<(), StorageError>;
    async fn record_rollback(&self, tenant_id: &str, sequence_name: &str, error_rate: f64, threshold: f64, reason: &str) -> Result<(), StorageError>;
    async fn query_error_rate(&self, tenant_id: &str, sequence_name: &str, window_secs: i64) -> Result<Option<f64>, StorageError>;
    async fn list_rollback_history(&self, tenant_id: Option<&str>, sequence_name: Option<&str>, limit: u32) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError>;

    // --- Health (pass-through) ---
    async fn ping(&self) -> Result<(), StorageError>;
    }
}

// ============================================================================
// Sub-trait 9: TelemetryStore -- pure pass-through. The decorator does not
// persist telemetry itself; delegating to the inner backend rather than
// relying on the trait's default no-op impls avoids silently discarding the
// inner backend's real implementation.
// ============================================================================

passthrough_impl! {
    impl crate::TelemetryStore for EncryptingStorage {
        async fn ingest_telemetry_event(&self, event_type: &str, payload: &str, device_id: &str, os_name: &str, os_version: &str, app_version: &str, sdk_version: &str, tenant_id: &str, created_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn ingest_telemetry_events_batch(&self, events: &[crate::TelemetryEvent]) -> Result<u64, StorageError>;
        async fn ingest_telemetry_error(&self, error_type: &str, message: &str, stack_trace: Option<&str>, device_id: &str, os_name: &str, os_version: &str, app_version: &str, sdk_version: &str, tenant_id: &str, instance_id: Option<&str>, sequence_name: Option<&str>) -> Result<(), StorageError>;
        async fn query_telemetry_dashboard(&self, query_type: &str, tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<(String, i64)>, StorageError>;
        async fn delete_old_telemetry_events(&self, older_than: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;
        async fn record_usage_event(&self, event: &crate::UsageEvent) -> Result<(), StorageError>;
        async fn query_usage(&self, tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<crate::UsageAggregate>, StorageError>;
        async fn query_instance_usage_totals(&self, instance_id: InstanceId) -> Result<(i64, i64), StorageError>;
    }
}

// ============================================================================
// Sub-trait 10: ResourceStore -- pure pass-through
// ============================================================================

passthrough_impl! {
    impl crate::ResourceStore for EncryptingStorage {
    // --- Artifacts ---
    // Artifact bytes are encrypted at rest (AES-256-GCM) in this wrapper before
    // they reach the object store, so blobs get the same protection as context
    // and credentials — independent of bucket-level SSE (which may be off, or
    // absent entirely on the local-filesystem backend).
    //
    // Encrypted blobs are framed with [`ARTIFACT_ENC_MAGIC`] so reads are
    // self-describing: a blob written *before* encryption was enabled (no magic)
    // is returned as-is rather than failing to decrypt. This makes turning the
    // encryption key on non-destructive for any pre-existing plaintext blobs.
    fn artifacts_enabled(&self) -> bool {
        self.inner.artifacts_enabled()
    }

    async fn put_artifact(
        &self,
        instance_id: InstanceId,
        content_type: &str,
        bytes: bytes::Bytes,
    ) -> Result<orch8_types::artifact::ArtifactRef, StorageError> {
        let plaintext_len = bytes.len() as u64;
        let ciphertext = self
            .encryptor
            .encrypt_bytes(&bytes)
            .map_err(|e| StorageError::Backend(format!("artifact encrypt: {e}")))?;
        let mut framed = Vec::with_capacity(ARTIFACT_ENC_MAGIC.len() + ciphertext.len());
        framed.extend_from_slice(ARTIFACT_ENC_MAGIC);
        framed.extend_from_slice(&ciphertext);
        let mut aref = self
            .inner
            .put_artifact(instance_id, content_type, bytes::Bytes::from(framed))
            .await?;
        // Report the *plaintext* size, not the on-disk ciphertext size, so the
        // ref the caller sees matches the bytes they put in.
        aref.size = plaintext_len;
        Ok(aref)
    }
    async fn get_artifact(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match self.inner.get_artifact(key).await? {
            Some(blob) => match blob.strip_prefix(ARTIFACT_ENC_MAGIC) {
                // Encrypted by this wrapper → decrypt.
                Some(ciphertext) => {
                    let plain = self
                        .encryptor
                        .decrypt_bytes(ciphertext)
                        .map_err(|e| StorageError::Backend(format!("artifact decrypt: {e}")))?;
                    Ok(Some(plain))
                }
                // No magic → stored before encryption was enabled; return as-is.
                None => Ok(Some(blob)),
            },
            None => Ok(None),
        }
    }
    async fn delete_artifact(&self, key: &str) -> Result<(), StorageError>;
    async fn list_artifacts(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::artifact::ArtifactMeta>, StorageError>;
    // Artifact deletion needs no crypto (delete-by-key is opaque), so delegate
    // straight to the inner backend. Explicit override rather than inheriting
    // the trait's provided method, to keep decorator coverage exhaustive.
    async fn delete_instance_artifacts(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
    async fn list_artifact_gc_candidates(&self, cutoff: chrono::DateTime<chrono::Utc>, limit: u32) -> Result<Vec<InstanceId>, StorageError>;
    async fn mark_artifacts_gced(&self, instance_id: InstanceId) -> Result<(), StorageError>;
    // No field on `TaskInstance`/`StepLog`/`AuditLogEntry` survives outside
    // `context.data` (already handled elsewhere) once the row is deleted, so
    // this is a pure pass-through -- nothing here to encrypt or decrypt.
    async fn delete_terminal_instances(&self, cutoff: chrono::DateTime<chrono::Utc>, limit: u32) -> Result<u64, StorageError>;

    // --- Instance KV State ---
    // Holds step-handler-defined state, including the agent step's
    // `__agent__:{block_id}` conversation-history checkpoint -- same data
    // class as `context.data`.
    async fn set_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_json_value(value)?;
        self.inner
            .set_instance_kv(instance_id, key, &encrypted)
            .await
    }
    async fn get_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        match self.inner.get_instance_kv(instance_id, key).await? {
            Some(v) => Ok(Some(self.decrypt_json_value(&v)?)),
            None => Ok(None),
        }
    }
    async fn get_all_instance_kv(
        &self,
        instance_id: InstanceId,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        let raw = self.inner.get_all_instance_kv(instance_id).await?;
        raw.into_iter()
            .map(|(k, v)| self.decrypt_json_value(&v).map(|dv| (k, dv)))
            .collect()
    }
    async fn delete_instance_kv(&self, instance_id: InstanceId, key: &str) -> Result<(), StorageError>;

    // --- Externalized State ---
    // Payloads pulled out of `context.data` by the externalization path carry
    // the same data class as inline context, so they get the same at-rest
    // protection here rather than landing in `externalized_state` as
    // plaintext.
    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_json_value(payload)?;
        self.inner
            .save_externalized_state(instance_id, ref_key, &encrypted)
            .await
    }
    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        match self.inner.get_externalized_state(ref_key).await? {
            Some(v) => Ok(Some(self.decrypt_json_value(&v)?)),
            None => Ok(None),
        }
    }
    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError>;

    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        let encrypted: Vec<(String, serde_json::Value)> = entries
            .iter()
            .map(|(k, v)| self.encrypt_json_value(v).map(|ev| (k.clone(), ev)))
            .collect::<Result<_, _>>()?;
        crate::ResourceStore::batch_save_externalized_state(&*self.inner, instance_id, &encrypted)
            .await
    }
    async fn batch_get_externalized_state(
        &self,
        ref_keys: &[String],
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        let raw = self.inner.batch_get_externalized_state(ref_keys).await?;
        raw.into_iter()
            .map(|(k, v)| self.decrypt_json_value(&v).map(|dv| (k, dv)))
            .collect()
    }
    async fn delete_expired_externalized_state(&self, limit: u32) -> Result<u64, StorageError>;

    // --- Resource Pools (pass-through) ---
    async fn create_resource_pool(&self, pool: &orch8_types::pool::ResourcePool) -> Result<(), StorageError>;
    async fn get_resource_pool(&self, id: Uuid) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError>;
    async fn list_resource_pools(&self, tenant_id: &orch8_types::ids::TenantId) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError>;
    async fn update_pool_round_robin_index(&self, pool_id: Uuid, index: u32) -> Result<(), StorageError>;
    async fn delete_resource_pool(&self, id: Uuid) -> Result<(), StorageError>;
    async fn add_pool_resource(&self, resource: &orch8_types::pool::PoolResource) -> Result<(), StorageError>;
    async fn list_pool_resources(&self, pool_id: Uuid) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError>;
    async fn update_pool_resource(&self, resource: &orch8_types::pool::PoolResource) -> Result<(), StorageError>;
    async fn delete_pool_resource(&self, id: Uuid) -> Result<(), StorageError>;
    async fn increment_resource_usage(&self, resource_id: Uuid, today: chrono::NaiveDate) -> Result<(), StorageError>;

    // --- Checkpoints ---
    // `checkpoint_data` holds a snapshot of execution state including agent
    // message history -- the same data class as `context.data`.
    async fn save_checkpoint(
        &self,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError> {
        if FieldEncryptor::is_encrypted(&checkpoint.checkpoint_data) {
            return self.inner.save_checkpoint(checkpoint).await;
        }
        let mut cp = checkpoint.clone();
        cp.checkpoint_data = self.encrypt_json_value(&cp.checkpoint_data)?;
        self.inner.save_checkpoint(&cp).await
    }
    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
        let mut cp = self.inner.get_latest_checkpoint(instance_id).await?;
        if let Some(ref mut c) = cp {
            c.checkpoint_data = self.decrypt_json_value(&c.checkpoint_data)?;
        }
        Ok(cp)
    }
    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
        let mut cps = self.inner.list_checkpoints(instance_id, limit).await?;
        for c in &mut cps {
            c.checkpoint_data = self.decrypt_json_value(&c.checkpoint_data)?;
        }
        Ok(cps)
    }
    async fn prune_checkpoints(&self, instance_id: InstanceId, keep: u32) -> Result<u64, StorageError>;
    }
}

passthrough_impl! {
    impl crate::MobileSyncStore for EncryptingStorage {
    // Device/status/approval methods are pass-through (no secret-bearing
    // columns). Only command payloads can carry resolved credentials, so those
    // are encrypted at rest and decrypted on the way out to the device (which
    // receives plaintext over TLS).

    async fn register_mobile_device(&self, device: &crate::MobileDevice) -> Result<(), StorageError>;
    async fn get_mobile_device(&self, device_id: &str) -> Result<Option<crate::MobileDevice>, StorageError>;
    async fn update_device_last_sync(&self, device_id: &str) -> Result<(), StorageError>;
    async fn list_mobile_devices(&self, tenant_id: Option<&str>, limit: u32) -> Result<Vec<crate::MobileDevice>, StorageError>;
    async fn mark_stale_devices_inactive(&self, stale_after_secs: i64) -> Result<u64, StorageError>;
    async fn upsert_mobile_instance_status(&self, status: &crate::MobileInstanceStatus) -> Result<(), StorageError>;
    async fn upsert_mobile_instance_status_batch(&self, statuses: &[crate::MobileInstanceStatus]) -> Result<(), StorageError>;
    async fn list_mobile_instance_status(&self, tenant_id: Option<&str>, device_id: Option<&str>, limit: u32) -> Result<Vec<crate::MobileInstanceStatus>, StorageError>;
    async fn insert_mobile_approval(&self, approval: &crate::MobileApprovalRequest) -> Result<bool, StorageError>;
    async fn get_mobile_approval(&self, id: &str) -> Result<Option<crate::MobileApprovalRequest>, StorageError>;
    async fn resolve_mobile_approval(&self, id: &str, resolution: &str) -> Result<Option<crate::MobileApprovalRequest>, StorageError>;
    async fn list_mobile_approvals(&self, tenant_id: Option<&str>, state: Option<&str>, limit: u32) -> Result<Vec<crate::MobileApprovalRequest>, StorageError>;
    async fn expire_mobile_approvals(&self) -> Result<u64, StorageError>;

    async fn create_mobile_command(
        &self,
        command: &crate::MobileCommand,
    ) -> Result<(), StorageError> {
        // Encrypt the payload at rest — it can contain resolved credentials
        // (a step delegation's params). The plaintext only ever lives in the
        // HTTPS sync response after fetch_pending_commands decrypts it.
        let mut encrypted = command.clone();
        encrypted.payload = self.encrypt_string_field(&command.payload)?;
        self.inner.create_mobile_command(&encrypted).await
    }

    async fn fetch_pending_commands(
        &self,
        device_id: &str,
        limit: u32,
    ) -> Result<Vec<crate::MobileCommand>, StorageError> {
        let mut commands = self.inner.fetch_pending_commands(device_id, limit).await?;
        for cmd in &mut commands {
            cmd.payload = self.decrypt_string_field(&cmd.payload)?;
        }
        Ok(commands)
    }

    async fn ack_mobile_commands(&self, device_id: &str, command_ids: &[String]) -> Result<u64, StorageError>;
    async fn cleanup_acked_commands(&self, older_than_secs: i64) -> Result<u64, StorageError>;
    async fn cleanup_expired_commands(&self, ttl_secs: i64) -> Result<u64, StorageError>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_encryptor() -> FieldEncryptor {
        FieldEncryptor::from_hex_key(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap()
    }

    #[test]
    fn is_encrypted_returns_true_for_encrypted_prefix() {
        let enc = test_encryptor();
        let encrypted = enc.encrypt_value(&json!({"secret": "data"})).unwrap();
        assert!(FieldEncryptor::is_encrypted(&encrypted));
    }

    #[test]
    fn is_encrypted_returns_false_for_plain_text() {
        assert!(!FieldEncryptor::is_encrypted(&json!("hello world")));
    }

    #[test]
    fn is_encrypted_returns_false_for_empty_string() {
        assert!(!FieldEncryptor::is_encrypted(&json!("")));
    }

    #[test]
    fn is_encrypted_returns_false_for_non_string_values() {
        assert!(!FieldEncryptor::is_encrypted(&json!(42)));
        assert!(!FieldEncryptor::is_encrypted(&json!(null)));
        assert!(!FieldEncryptor::is_encrypted(&json!(true)));
        assert!(!FieldEncryptor::is_encrypted(&json!([1, 2, 3])));
        assert!(!FieldEncryptor::is_encrypted(&json!({"key": "value"})));
    }

    #[test]
    fn decrypt_value_leaves_plain_string_unchanged() {
        let enc = test_encryptor();
        let plain = json!("not encrypted");
        let result = enc.decrypt_value(&plain).unwrap();
        assert_eq!(result, plain);
    }

    #[test]
    fn decrypt_value_leaves_empty_string_unchanged() {
        let enc = test_encryptor();
        let empty = json!("");
        let result = enc.decrypt_value(&empty).unwrap();
        assert_eq!(result, empty);
    }

    #[test]
    fn decrypt_value_leaves_non_string_unchanged() {
        let enc = test_encryptor();
        let obj = json!({"a": 1});
        assert_eq!(enc.decrypt_value(&obj).unwrap(), obj);

        let arr = json!([1, 2, 3]);
        assert_eq!(enc.decrypt_value(&arr).unwrap(), arr);

        let num = json!(42);
        assert_eq!(enc.decrypt_value(&num).unwrap(), num);

        let nul = json!(null);
        assert_eq!(enc.decrypt_value(&nul).unwrap(), nul);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_object() {
        let enc = test_encryptor();
        let original = json!({"user": "alice", "count": 7, "nested": {"ok": true}});
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_string() {
        let enc = test_encryptor();
        let original = json!("sensitive payload");
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_number() {
        let enc = test_encryptor();
        let original = json!(-123.456);
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_null() {
        let enc = test_encryptor();
        let original = json!(null);
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_array() {
        let enc = test_encryptor();
        let original = json!([1, "two", null, {"three": 3}]);
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypted_value_has_prefix() {
        let enc = test_encryptor();
        let encrypted = enc.encrypt_value(&json!("x")).unwrap();
        let s = encrypted.as_str().unwrap();
        assert!(s.starts_with("enc:v1:"));
    }

    #[test]
    fn encrypt_produces_different_ciphertext_each_time() {
        let enc = test_encryptor();
        let v = json!({"secret": "data"});
        let a = enc.encrypt_value(&v).unwrap();
        let b = enc.encrypt_value(&v).unwrap();
        assert_ne!(a, b, "nonce randomness should produce distinct ciphertexts");
    }

    #[tokio::test]
    async fn artifact_bytes_are_encrypted_at_rest() {
        use crate::ResourceStore;
        use crate::artifacts::ObjectArtifactStore;
        use std::sync::Arc;

        let plain = b"\x89PNG sensitive user document".to_vec();
        let inner: Arc<dyn crate::StorageBackend> = Arc::new(
            crate::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        let enc = EncryptingStorage::new(Arc::clone(&inner), test_encryptor());

        let aref = enc
            .put_artifact(
                InstanceId::new(),
                "image/png",
                bytes::Bytes::from(plain.clone()),
            )
            .await
            .unwrap();

        // The wrapper decrypts on read → caller sees plaintext.
        assert_eq!(enc.get_artifact(&aref.key).await.unwrap().unwrap(), plain);
        // The ref reports the PLAINTEXT size, not the ciphertext size (B2).
        assert_eq!(aref.size, plain.len() as u64);
        // The underlying store holds magic-framed ciphertext, NOT plaintext.
        let at_rest = inner.get_artifact(&aref.key).await.unwrap().unwrap();
        assert_ne!(at_rest, plain, "artifact must not be stored in plaintext");
        assert!(
            at_rest.starts_with(ARTIFACT_ENC_MAGIC),
            "encrypted blobs are framed with the magic header"
        );
    }

    #[tokio::test]
    async fn pre_encryption_plaintext_artifact_reads_back_unchanged() {
        // B1: a blob written before encryption was enabled (no magic header)
        // must read back as-is through the encrypting wrapper, not error.
        use crate::ResourceStore;
        use crate::artifacts::ObjectArtifactStore;
        use std::sync::Arc;

        let plain = b"legacy plaintext blob".to_vec();
        let inner: Arc<dyn crate::StorageBackend> = Arc::new(
            crate::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        // Write WITHOUT the encrypting wrapper (simulates a pre-encryption blob).
        let aref = inner
            .put_artifact(
                InstanceId::new(),
                "text/plain",
                bytes::Bytes::from(plain.clone()),
            )
            .await
            .unwrap();

        let enc = EncryptingStorage::new(Arc::clone(&inner), test_encryptor());
        // Reading through the wrapper returns the plaintext as-is (no decrypt error).
        assert_eq!(enc.get_artifact(&aref.key).await.unwrap().unwrap(), plain);
    }

    #[tokio::test]
    async fn artifacts_enabled_reflects_backend_and_delegates() {
        // B3: artifacts_enabled() must be false without a backend, true with one,
        // and the encrypting wrapper delegates to inner.
        use crate::ResourceStore;
        use crate::artifacts::ObjectArtifactStore;
        use std::sync::Arc;

        let no_backend = crate::sqlite::SqliteStorage::in_memory().await.unwrap();
        assert!(!no_backend.artifacts_enabled());

        let with_backend: Arc<dyn crate::StorageBackend> = Arc::new(
            crate::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        assert!(with_backend.artifacts_enabled());
        let enc = EncryptingStorage::new(with_backend, test_encryptor());
        assert!(enc.artifacts_enabled(), "wrapper must delegate to inner");
    }
}
