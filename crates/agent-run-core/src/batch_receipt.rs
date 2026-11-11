//! Read-only, database-scoped lookup of exact permanent batch receipts.

use crate::{Error, Result, service::Service};
use agent_run_domain::batch::{BatchReceiptLookup, StartBatchRequest};

impl Service {
    /// Looks up the exact normalized batch intent without configuration or admission.
    /// A missing receipt is only a snapshot fact; an in-flight admission may still commit.
    pub async fn batch_receipt(
        &self,
        mut request: StartBatchRequest,
    ) -> Result<BatchReceiptLookup> {
        request.validate_receipt_lookup()?;
        let fingerprint = request.fingerprint()?;
        self.read(move |store| {
            let receipt = store
                .replay_batch_scoped(
                    &request.request_id,
                    &fingerprint,
                    request.expected_store_id.as_ref(),
                )?
                .map(|result| result.receipt);
            let result = BatchReceiptLookup {
                found: receipt.is_some(),
                receipt,
            };
            result
                .validate_for(&request)
                .map_err(|_| Error::Integrity("invalid batch receipt lookup".into()))?;
            if serde_json::to_vec(&result)?.len()
                > agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES - 8192
            {
                return Err(Error::Runtime("batch receipt exceeds frame budget".into()));
            }
            Ok(result)
        })
        .await
    }
}
