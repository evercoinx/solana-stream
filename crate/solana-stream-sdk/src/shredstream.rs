//! Shredstream client wrapper

use std::collections::HashMap;

use tonic::transport::Channel;

use crate::{
    Result, SolanaStreamError,
    shredstream_proto::{
        CommitmentLevel, SubscribeEntriesRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
        shredstream_proxy_client::ShredstreamProxyClient,
    },
};

/// A convenient wrapper around the Shredstream client
pub struct ShredstreamClient {
    client: ShredstreamProxyClient<Channel>,
}

impl ShredstreamClient {
    /// Create a new ShredstreamClient by connecting to the specified endpoint
    ///
    /// # Arguments
    /// * `endpoint` - The gRPC endpoint URL (e.g., "https://shreds-ams.erpc.global")
    ///
    /// # Example
    /// ```no_run
    /// use solana_stream_sdk::ShredstreamClient;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = ShredstreamClient::connect("https://shreds-ams.erpc.global").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self> {
        let client = ShredstreamProxyClient::connect(endpoint.as_ref().to_string())
            .await
            .map_err(SolanaStreamError::Transport)?;

        Ok(Self { client })
    }

    /// Subscribe to entries with the given filters
    ///
    /// # Arguments
    /// * `request` - The subscribe entries request
    pub async fn subscribe_entries(
        &mut self,
        request: SubscribeEntriesRequest,
    ) -> Result<tonic::Streaming<crate::shredstream_proto::Entry>> {
        let response = self
            .client
            .subscribe_entries(request)
            .await
            .map_err(SolanaStreamError::Status)?;

        Ok(response.into_inner())
    }

    /// Create a simple entries subscription request with single account filter
    ///
    /// # Arguments
    /// * `account` - The account address to filter for
    /// * `commitment` - The commitment level (optional, defaults to Processed)
    ///
    /// # Example
    /// ```no_run
    /// use solana_stream_sdk::{CommitmentLevel, ShredstreamClient};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let mut client = ShredstreamClient::connect("https://shreds-ams.erpc.global").await?;
    ///     let request = ShredstreamClient::create_entries_request_for_account(
    ///         "L1ocbjmuFUQDVwwUWi8HjXjg1RYEeN58qQx6iouAsGF",
    ///         Some(CommitmentLevel::Processed),
    ///     );
    ///     let mut stream = client.subscribe_entries(request).await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn create_entries_request_for_account(
        account: impl AsRef<str>,
        commitment: Option<CommitmentLevel>,
    ) -> SubscribeEntriesRequest {
        let mut accounts = HashMap::new();
        accounts.insert(
            "".to_owned(),
            SubscribeRequestFilterAccounts {
                account: vec![account.as_ref().to_owned()],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );

        let mut transactions = HashMap::new();
        transactions.insert(
            "".to_owned(),
            SubscribeRequestFilterTransactions {
                account_include: vec!["".to_owned()],
                account_exclude: vec!["".to_owned()],
                account_required: vec!["".to_owned()],
            },
        );

        let mut slots = HashMap::new();
        slots.insert(
            "".to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        );

        SubscribeEntriesRequest {
            accounts,
            transactions,
            slots,
            commitment: Some(commitment.unwrap_or(CommitmentLevel::Processed) as i32),
        }
    }

    /// Create entries subscription request with multiple accounts, owners, and filters
    pub fn create_entries_request_for_accounts(
        accounts: Vec<String>,
        owners: Vec<String>,
        filters: Vec<crate::shredstream_proto::SubscribeRequestFilterAccountsFilter>,
        commitment: Option<CommitmentLevel>,
    ) -> SubscribeEntriesRequest {
        let mut account_filters = HashMap::new();
        account_filters.insert(
            "".to_owned(),
            SubscribeRequestFilterAccounts {
                account: accounts,
                owner: owners,
                filters,
                nonempty_txn_signature: None,
            },
        );

        let mut transactions = HashMap::new();
        transactions.insert(
            "".to_owned(),
            SubscribeRequestFilterTransactions {
                account_include: vec!["".to_owned()],
                account_exclude: vec!["".to_owned()],
                account_required: vec!["".to_owned()],
            },
        );

        let mut slots = HashMap::new();
        slots.insert(
            "".to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        );

        SubscribeEntriesRequest {
            accounts: account_filters,
            transactions,
            slots,
            commitment: Some(commitment.unwrap_or(CommitmentLevel::Processed) as i32),
        }
    }

    /// Create an empty entries subscription request that can be customized
    pub fn create_empty_entries_request() -> SubscribeEntriesRequest {
        SubscribeEntriesRequest {
            accounts: HashMap::new(),
            transactions: HashMap::new(),
            slots: HashMap::new(),
            commitment: Some(CommitmentLevel::Processed as i32),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_for_account_populates_filter() {
        let account = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
        let req = ShredstreamClient::create_entries_request_for_account(
            account,
            Some(CommitmentLevel::Confirmed),
        );

        let filter = req.accounts.get("").unwrap();
        assert!(filter.account.contains(&account.to_owned()));
        assert_eq!(
            req.commitment,
            Some(CommitmentLevel::Confirmed as i32)
        );
        let slot_filter = req.slots.get("").unwrap();
        assert_eq!(slot_filter.filter_by_commitment, Some(true));
        assert_eq!(slot_filter.interslot_updates, Some(false));
    }

    #[test]
    fn request_for_account_defaults_to_processed() {
        let req = ShredstreamClient::create_entries_request_for_account(
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
            None,
        );
        assert_eq!(req.commitment, Some(CommitmentLevel::Processed as i32));
    }

    #[test]
    fn request_for_accounts_multi_fields() {
        let accs = vec!["acc1".to_owned(), "acc2".to_owned()];
        let owners = vec!["owner1".to_owned()];
        let req = ShredstreamClient::create_entries_request_for_accounts(
            accs.clone(),
            owners.clone(),
            vec![],
            Some(CommitmentLevel::Finalized),
        );

        let filter = req.accounts.get("").unwrap();
        assert_eq!(filter.account, accs);
        assert_eq!(filter.owner, owners);
        assert_eq!(req.commitment, Some(CommitmentLevel::Finalized as i32));
    }

    #[test]
    fn empty_request_has_empty_maps_and_processed() {
        let req = ShredstreamClient::create_empty_entries_request();
        assert!(req.accounts.is_empty());
        assert!(req.transactions.is_empty());
        assert!(req.slots.is_empty());
        assert_eq!(req.commitment, Some(CommitmentLevel::Processed as i32));
    }
}
