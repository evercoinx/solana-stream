use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    str::FromStr,
    sync::Arc,
};

use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::VersionedTransaction};
use solana_vote_program::id as vote_program_id;

const TOKEN_PROGRAM: Pubkey = solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const TOKEN_2022_PROGRAM: Pubkey =
    solana_sdk::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

#[inline]
fn is_token_program(pk: &Pubkey) -> bool {
    *pk == TOKEN_PROGRAM || *pk == TOKEN_2022_PROGRAM
}
const DEFAULT_PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPFUN_CREATE_DISC: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
const PUMPFUN_CREATE_V2_DISC: [u8; 8] = [0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4];
const PUMPFUN_BUY_DISC: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
const PUMPFUN_BUY_EXACT_SOL_IN_DISC: [u8; 8] = [0x38, 0xfc, 0x74, 0x08, 0x9e, 0xdf, 0xcd, 0x5f];
const PUMPFUN_SELL_DISC: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];

#[derive(Clone)]
pub struct ProgramWatchConfig {
    pub program_ids: HashSet<Pubkey>,
    pub authorities: HashSet<Pubkey>,
    pub fee_payers: HashSet<Pubkey>,
    pub token_program_ids: HashSet<Pubkey>,
    pub skip_vote_txs: bool,
    pub mint_finder: Arc<dyn MintFinder + Send + Sync>,
    pub detailers: Vec<Arc<dyn MintDetailer + Send + Sync>>,
}

impl ProgramWatchConfig {
    pub fn new(program_ids: Vec<Pubkey>, authorities: Vec<Pubkey>) -> Self {
        let program_ids_set: HashSet<Pubkey> = program_ids.iter().copied().collect();
        Self {
            mint_finder: Arc::new(default_mint_finder(&program_ids)),
            detailers: default_detailers_from_programs(&program_ids),
            program_ids: program_ids_set,
            authorities: authorities.into_iter().collect(),
            fee_payers: HashSet::new(),
            token_program_ids: default_token_program_ids().into_iter().collect(),
            skip_vote_txs: true,
        }
    }

    pub fn with_token_program_ids(mut self, token_program_ids: Vec<Pubkey>) -> Self {
        self.token_program_ids = token_program_ids.into_iter().collect();
        self
    }

    pub fn with_fee_payers(mut self, fee_payers: Vec<Pubkey>) -> Self {
        self.fee_payers = fee_payers.into_iter().collect();
        self
    }

    pub fn with_skip_vote_txs(mut self, skip_vote_txs: bool) -> Self {
        self.skip_vote_txs = skip_vote_txs;
        self
    }

    pub fn with_mint_finder(mut self, mint_finder: Arc<dyn MintFinder + Send + Sync>) -> Self {
        self.mint_finder = mint_finder;
        self
    }

    pub fn with_detailers(mut self, detailers: Vec<Arc<dyn MintDetailer + Send + Sync>>) -> Self {
        self.detailers = detailers;
        self
    }
}

#[derive(Debug, Clone)]
pub struct ProgramHit {
    pub signature: Signature,
    pub fee_payer: Pubkey,
    pub program_hit: bool,
    pub authority_hit: bool,
    pub mints: Vec<MintInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MintInfo {
    pub mint: Pubkey,
    pub label: Option<&'static str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MintDetail {
    pub mint: Pubkey,
    pub label: Option<&'static str>,
    pub action: Option<&'static str>,
    pub sol_amount: Option<u64>,
    pub token_amount: Option<u64>,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub uri: Option<String>,
    pub creator: Option<Pubkey>,
    pub is_mayhem_mode: Option<bool>,
    pub is_cashback_coin: Option<bool>,
    pub token_program: Option<Pubkey>,
}

pub fn parse_pubkeys_env(var: &str, defaults: &[&str]) -> Vec<Pubkey> {
    let raw = std::env::var(var).ok();
    parse_pubkeys(raw.as_deref(), defaults)
}

pub fn parse_pubkeys(raw: Option<&str>, defaults: &[&str]) -> Vec<Pubkey> {
    let mut set = BTreeSet::new();

    match raw {
        Some(value) => {
            for part in value.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(pk) = Pubkey::from_str(trimmed) {
                    set.insert(pk);
                }
            }
        }
        None => {
            for def in defaults {
                if let Ok(pk) = Pubkey::from_str(def) {
                    set.insert(pk);
                }
            }
        }
    }

    set.into_iter().collect()
}

pub fn default_token_program_ids() -> Vec<Pubkey> {
    vec![TOKEN_PROGRAM, TOKEN_2022_PROGRAM]
}

pub fn is_vote_transaction(tx: &VersionedTransaction) -> bool {
    let keys = tx.message.static_account_keys();
    tx.message.instructions().iter().any(|ix| {
        keys.get(ix.program_id_index as usize)
            .map(|pid| pid == &vote_program_id())
            .unwrap_or(false)
    })
}

#[must_use]
pub fn detect_program_hit(
    tx: &VersionedTransaction,
    cfg: &ProgramWatchConfig,
) -> Option<ProgramHit> {
    if cfg.program_ids.is_empty() && cfg.authorities.is_empty() {
        return None;
    }
    let keys = tx.message.static_account_keys();

    if !cfg.fee_payers.is_empty() {
        let fee_payer_matches = keys
            .first()
            .map(|fp| cfg.fee_payers.contains(fp))
            .unwrap_or(false);
        if !fee_payer_matches {
            return None;
        }
    }

    let authority_hit =
        !cfg.authorities.is_empty() && keys.iter().any(|key| cfg.authorities.contains(key));
    let mut program_hit = false;

    for ix in tx.message.instructions() {
        let Some(program_id) = keys.get(ix.program_id_index as usize) else {
            continue;
        };
        if cfg.skip_vote_txs && *program_id == vote_program_id() {
            return None;
        }
        if !program_hit && cfg.program_ids.contains(program_id) {
            program_hit = true;
        }
    }

    if !program_hit && !authority_hit {
        return None;
    }

    let mint_accounts: Vec<MintInfo> = cfg.mint_finder.find_mints(tx, cfg);

    Some(ProgramHit {
        signature: tx.signatures.first().cloned().unwrap_or_default(),
        fee_payer: *keys
            .first()
            .expect("valid transaction must have fee payer at account_keys[0]"),
        program_hit,
        authority_hit,
        mints: mint_accounts,
    })
}

pub fn first_signatures<'a, I>(txs: I, limit: usize, skip_vote_txs: bool) -> Vec<Signature>
where
    I: IntoIterator<Item = &'a VersionedTransaction>,
{
    txs.into_iter()
        .filter(|tx| !(skip_vote_txs && is_vote_transaction(tx)))
        .filter_map(|tx| tx.signatures.first())
        .take(limit)
        .cloned()
        .collect()
}

pub trait MintFinder {
    fn find_mints(&self, tx: &VersionedTransaction, cfg: &ProgramWatchConfig) -> Vec<MintInfo>;
}

pub trait MintDetailer {
    fn detail(
        &self,
        tx: &VersionedTransaction,
        cfg: &ProgramWatchConfig,
        mints: &[MintInfo],
    ) -> Vec<MintDetail>;
}

fn insert_mint(map: &mut BTreeMap<Pubkey, MintInfo>, mint: Pubkey, label: Option<&'static str>) {
    map.entry(mint)
        .and_modify(|info| {
            if info.label.is_none() && label.is_some() {
                info.label = label;
            }
        })
        .or_insert(MintInfo { mint, label });
}

/// Default SPL Token/Token-2022 finder: looks for Initialize/MintTo tags at accounts[0].
pub struct SplTokenMintFinder;

impl MintFinder for SplTokenMintFinder {
    fn find_mints(&self, tx: &VersionedTransaction, cfg: &ProgramWatchConfig) -> Vec<MintInfo> {
        let mut mint_accounts = BTreeMap::new();
        let keys = tx.message.static_account_keys();
        for ix in tx.message.instructions() {
            let Some(program_id) = keys.get(ix.program_id_index as usize) else {
                continue;
            };
            if !cfg.token_program_ids.contains(program_id) {
                continue;
            }
            let Some(tag) = ix.data.first() else {
                continue;
            };
            if matches!(tag, 0 | 7 | 14 | 20)
                && let Some(mint_idx) = ix.accounts.first()
                && let Some(mint) = keys.get(*mint_idx as usize)
            {
                insert_mint(&mut mint_accounts, *mint, Some("spl-token"));
            }
        }
        mint_accounts.into_values().collect()
    }
}

/// Pump.fun finder: picks mint from pump.fun instructions by account position (create_v2/buy/sell).
pub struct PumpfunAccountMintFinder {
    pumpfun_ids: HashSet<Pubkey>,
}

impl PumpfunAccountMintFinder {
    pub fn new(pumpfun_ids: Vec<Pubkey>) -> Self {
        Self {
            pumpfun_ids: pumpfun_ids.into_iter().collect(),
        }
    }
}

const SYSTEM_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("11111111111111111111111111111111");

fn is_system_id(pk: &Pubkey) -> bool {
    *pk == SYSTEM_PROGRAM_ID
}

impl MintFinder for PumpfunAccountMintFinder {
    fn find_mints(&self, tx: &VersionedTransaction, _cfg: &ProgramWatchConfig) -> Vec<MintInfo> {
        let keys = tx.message.static_account_keys();
        let mut mints = BTreeMap::new();
        for ix in tx.message.instructions() {
            let Some(program_id) = keys.get(ix.program_id_index as usize) else {
                continue;
            };
            if !self.pumpfun_ids.contains(program_id) {
                continue;
            }
            let kind = match ix.data.get(0..8) {
                Some(bytes) if bytes == PUMPFUN_CREATE_V2_DISC || bytes == PUMPFUN_CREATE_DISC => {
                    Some("pump:create")
                }
                Some(bytes) if bytes == PUMPFUN_BUY_DISC => Some("pump:buy"),
                Some(bytes) if bytes == PUMPFUN_BUY_EXACT_SOL_IN_DISC => Some("pump:buy_exact"),
                Some(bytes) if bytes == PUMPFUN_SELL_DISC => Some("pump:sell"),
                _ => None,
            };

            match kind {
                Some("pump:create") => {
                    if let Some(mint_idx) = ix.accounts.first()
                        && let Some(mint) = keys.get(*mint_idx as usize)
                        && !is_system_id(mint)
                    {
                        insert_mint(&mut mints, *mint, Some("pump:create"));
                    }
                }
                Some("pump:buy") | Some("pump:buy_exact") | Some("pump:sell") => {
                    if let Some(mint_idx) = ix.accounts.get(2)
                        && let Some(mint) = keys.get(*mint_idx as usize)
                        && !is_system_id(mint)
                    {
                        insert_mint(&mut mints, *mint, kind);
                    }
                }
                _ => {
                    if let Some(mint_idx) = ix.accounts.get(2)
                        && let Some(mint) = keys.get(*mint_idx as usize)
                        && !is_system_id(mint)
                    {
                        insert_mint(&mut mints, *mint, Some("pump:trade"));
                    }
                }
            }
        }
        mints.into_values().collect()
    }
}

/// Parses `creator`, `is_mayhem_mode`, and `is_cashback_coin` from a pump.fun CreateV2 instruction.
///
/// Layout (after the 8-byte discriminator):
/// `name(4+n) + symbol(4+n) + uri(4+n) + creator(32) + [is_mayhem_mode(1)] + is_cashback_coin(1)`
fn parse_create_v2_creator(data: &[u8]) -> Option<(Pubkey, bool, bool)> {
    let mut pos = 8usize; // skip discriminator
    for _ in 0..3 {
        // Skip name, symbol, uri
        let len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos = pos.checked_add(4 + len)?;
    }
    let creator = Pubkey::from(<[u8; 32]>::try_from(data.get(pos..pos + 32)?).ok()?);
    let is_mayhem_mode = data.get(pos + 32).copied().unwrap_or(0) != 0;
    let is_cashback_coin = data.get(pos + 33).copied().unwrap_or(0) != 0;
    Some((creator, is_mayhem_mode, is_cashback_coin))
}

/// Pump.fun detailer: adds action metadata based on label.
pub struct PumpfunDetailer {
    pumpfun_ids: HashSet<Pubkey>,
}

impl PumpfunDetailer {
    pub fn new(pumpfun_ids: Vec<Pubkey>) -> Self {
        Self {
            pumpfun_ids: pumpfun_ids.into_iter().collect(),
        }
    }
}

impl MintDetailer for PumpfunDetailer {
    fn detail(
        &self,
        tx: &VersionedTransaction,
        _cfg: &ProgramWatchConfig,
        mints: &[MintInfo],
    ) -> Vec<MintDetail> {
        let keys = tx.message.static_account_keys();
        let mut out = BTreeMap::new();
        for ix in tx.message.instructions() {
            let Some(program_id) = keys.get(ix.program_id_index as usize) else {
                continue;
            };
            if !self.pumpfun_ids.contains(program_id) {
                continue;
            }

            let disc = ix.data.get(0..8);
            let is_create_v2 = disc == Some(&PUMPFUN_CREATE_V2_DISC);
            let kind = match disc {
                Some(bytes) if bytes == PUMPFUN_CREATE_V2_DISC || bytes == PUMPFUN_CREATE_DISC => {
                    Some("create")
                }
                Some(bytes) if bytes == PUMPFUN_BUY_DISC => Some("buy"),
                Some(bytes) if bytes == PUMPFUN_BUY_EXACT_SOL_IN_DISC => Some("buy_exact"),
                Some(bytes) if bytes == PUMPFUN_SELL_DISC => Some("sell"),
                _ => None,
            };
            let mint_idx = match kind {
                Some("create") => ix.accounts.first(),
                _ => ix.accounts.get(2),
            };
            let Some(mint_idx) = mint_idx else {
                continue;
            };
            let Some(mint) = keys.get(*mint_idx as usize) else {
                continue;
            };
            if is_system_id(mint) {
                continue;
            }
            if kind.is_none() {
                continue;
            }
            let (token_amount, sol_amount) = match kind {
                Some("create") => (None, None),
                Some("buy_exact") => (
                    None,
                    ix.data
                        .get(8..16)
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes),
                ),
                _ => (
                    ix.data
                        .get(8..16)
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes),
                    ix.data
                        .get(16..24)
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes),
                ),
            };
            let (action, label) = match kind {
                Some("create") => (Some("create"), Some("pump:create")),
                Some("buy") => (Some("buy"), Some("pump:buy")),
                Some("buy_exact") => (Some("buy"), Some("pump:buy_exact")),
                Some("sell") => (Some("sell"), Some("pump:sell")),
                _ => (None, None),
            };

            let (creator, is_mayhem_mode, is_cashback_coin) = if is_create_v2 {
                parse_create_v2_creator(&ix.data).map_or((None, None, None), |(c, m, cb)| {
                    (Some(c), Some(m), Some(cb))
                })
            } else {
                (None, None, None)
            };

            let token_program = ix.accounts.iter().find_map(|&idx| {
                keys.get(idx as usize)
                    .filter(|pk| is_token_program(pk))
                    .copied()
            });
            let token_program = if token_program.is_none() && is_create_v2 {
                Some(TOKEN_2022_PROGRAM)
            } else {
                token_program
            };

            let entry = out.entry(*mint).or_insert(MintDetail {
                mint: *mint,
                label,
                action,
                sol_amount,
                token_amount,
                name: None,
                symbol: None,
                uri: None,
                creator,
                is_mayhem_mode,
                is_cashback_coin,
                token_program,
            });
            if let Some(k) = action
                && (entry.action.is_none() || entry.action == Some("create"))
            {
                entry.action = Some(k);
                entry.label = label;
            }
            if sol_amount.is_some() {
                entry.sol_amount = sol_amount;
            }
            if token_amount.is_some() {
                entry.token_amount = token_amount;
            }
            if creator.is_some() && entry.creator.is_none() {
                entry.creator = creator;
                entry.is_mayhem_mode = is_mayhem_mode;
                entry.is_cashback_coin = is_cashback_coin;
            }
            if token_program.is_some() && entry.token_program.is_none() {
                entry.token_program = token_program;
            }
        }

        for m in mints {
            out.entry(m.mint).or_insert_with(|| {
                let action = m.label.map(|l| {
                    let trimmed = l.trim_start_matches("pump:");
                    match trimmed {
                        "trade" => "sell",
                        "buy_exact" => "buy",
                        other => other,
                    }
                });
                MintDetail {
                    mint: m.mint,
                    label: m.label,
                    action,
                    sol_amount: None,
                    token_amount: None,
                    name: None,
                    symbol: None,
                    uri: None,
                    creator: None,
                    is_mayhem_mode: None,
                    is_cashback_coin: None,
                    token_program: None,
                }
            });
        }
        out.into_values().collect()
    }
}

/// Composite finder: aggregates multiple strategies.
pub struct CompositeMintFinder {
    finders: Vec<Arc<dyn MintFinder + Send + Sync>>,
}

impl CompositeMintFinder {
    pub fn new(finders: Vec<Arc<dyn MintFinder + Send + Sync>>) -> Self {
        Self { finders }
    }
}

impl MintFinder for CompositeMintFinder {
    fn find_mints(&self, tx: &VersionedTransaction, cfg: &ProgramWatchConfig) -> Vec<MintInfo> {
        let mut out = BTreeMap::new();
        for f in &self.finders {
            for m in f.find_mints(tx, cfg) {
                insert_mint(&mut out, m.mint, m.label);
            }
        }
        out.into_values().collect()
    }
}

fn pumpfun_ids_or_default(program_ids: &[Pubkey]) -> Vec<Pubkey> {
    if program_ids.is_empty() {
        Pubkey::from_str(DEFAULT_PUMPFUN_PROGRAM_ID)
            .map(|id| vec![id])
            .unwrap_or_default()
    } else {
        program_ids.to_vec()
    }
}

fn default_mint_finder(program_ids: &[Pubkey]) -> CompositeMintFinder {
    let pumpfun_ids = pumpfun_ids_or_default(program_ids);
    CompositeMintFinder::new(vec![
        Arc::new(PumpfunAccountMintFinder::new(pumpfun_ids)),
        Arc::new(SplTokenMintFinder),
    ])
}

pub fn default_detailers_from_programs(
    program_ids: &[Pubkey],
) -> Vec<Arc<dyn MintDetailer + Send + Sync>> {
    let pumpfun_ids = pumpfun_ids_or_default(program_ids);
    vec![Arc::new(PumpfunDetailer::new(pumpfun_ids))]
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use solana_sdk::{
        hash::Hash,
        message::compiled_instruction::CompiledInstruction,
        message::{Message, MessageHeader, VersionedMessage},
        pubkey::Pubkey,
        signature::Signature,
        transaction::VersionedTransaction,
    };

    use super::{MintDetail, MintDetailer, MintFinder, MintInfo, ProgramWatchConfig};

    pub fn make_tx(
        account_keys: Vec<Pubkey>,
        instructions: Vec<CompiledInstruction>,
    ) -> VersionedTransaction {
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let message = Message {
            header,
            account_keys,
            recent_blockhash: Hash::default(),
            instructions,
        };
        VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::Legacy(message),
        }
    }

    pub struct NoopMintFinder;

    impl MintFinder for NoopMintFinder {
        fn find_mints(
            &self,
            _tx: &VersionedTransaction,
            _cfg: &ProgramWatchConfig,
        ) -> Vec<MintInfo> {
            vec![]
        }
    }

    pub struct FixedMintFinder {
        pub mints: Vec<MintInfo>,
    }

    impl MintFinder for FixedMintFinder {
        fn find_mints(
            &self,
            _tx: &VersionedTransaction,
            _cfg: &ProgramWatchConfig,
        ) -> Vec<MintInfo> {
            self.mints.clone()
        }
    }

    pub struct NoopDetailer;

    impl MintDetailer for NoopDetailer {
        fn detail(
            &self,
            _tx: &VersionedTransaction,
            _cfg: &ProgramWatchConfig,
            _mints: &[MintInfo],
        ) -> Vec<MintDetail> {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use solana_sdk::{message::compiled_instruction::CompiledInstruction, pubkey::Pubkey};
    use solana_vote_program::id as vote_program_id;

    use super::*;
    use test_helpers::{FixedMintFinder, NoopDetailer, NoopMintFinder, make_tx};

    #[test]
    fn parse_pubkeys_none_uses_defaults() {
        let pk = Pubkey::new_from_array([1u8; 32]);
        let result = parse_pubkeys(None, &[&pk.to_string()]);
        assert_eq!(result, vec![pk]);
    }

    #[test]
    fn parse_pubkeys_some_overrides_defaults() {
        let pk1 = Pubkey::new_from_array([1u8; 32]);
        let pk2 = Pubkey::new_from_array([2u8; 32]);
        let default = Pubkey::new_from_array([9u8; 32]);
        let raw = format!("{},{}", pk1, pk2);
        let result = parse_pubkeys(Some(&raw), &[&default.to_string()]);
        assert!(result.contains(&pk1));
        assert!(result.contains(&pk2));
        assert!(!result.contains(&default));
    }

    #[test]
    fn parse_pubkeys_empty_string_returns_empty() {
        let result = parse_pubkeys(Some(""), &["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_pubkeys_trims_whitespace() {
        let pk1 = Pubkey::new_from_array([1u8; 32]);
        let pk2 = Pubkey::new_from_array([2u8; 32]);
        let raw = format!("  {} , {}  ", pk1, pk2);
        let result = parse_pubkeys(Some(&raw), &[]);
        assert!(result.contains(&pk1));
        assert!(result.contains(&pk2));
    }

    #[test]
    fn parse_pubkeys_skips_invalid() {
        let pk = Pubkey::new_from_array([1u8; 32]);
        let raw = format!("not-a-pubkey,{}", pk);
        let result = parse_pubkeys(Some(&raw), &[]);
        assert_eq!(result, vec![pk]);
    }

    #[test]
    fn parse_pubkeys_deduplicates() {
        let pk = Pubkey::new_from_array([1u8; 32]);
        let raw = format!("{},{}", pk, pk);
        let result = parse_pubkeys(Some(&raw), &[]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn vote_tx_detected() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let vote_prog = vote_program_id();
        let keys = vec![fee_payer, vote_prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        assert!(is_vote_transaction(&tx));
    }

    #[test]
    fn non_vote_tx_not_detected() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let other_prog = Pubkey::new_from_array([7u8; 32]);
        let keys = vec![fee_payer, other_prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        assert!(!is_vote_transaction(&tx));
    }

    fn watch_cfg_with(
        program_ids: Vec<Pubkey>,
        authorities: Vec<Pubkey>,
        finder: Arc<dyn MintFinder + Send + Sync>,
    ) -> ProgramWatchConfig {
        ProgramWatchConfig::new(program_ids, authorities)
            .with_skip_vote_txs(true)
            .with_mint_finder(finder)
            .with_detailers(vec![Arc::new(NoopDetailer)])
    }

    #[test]
    fn detect_returns_none_when_config_empty() {
        let cfg = watch_cfg_with(vec![], vec![], Arc::new(NoopMintFinder));
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let prog = Pubkey::new_from_array([1u8; 32]);
        let keys = vec![fee_payer, prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        assert!(detect_program_hit(&tx, &cfg).is_none());
    }

    #[test]
    fn detect_hits_on_program_id() {
        let prog = Pubkey::new_from_array([5u8; 32]);
        let cfg = watch_cfg_with(vec![prog], vec![], Arc::new(NoopMintFinder));
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        let hit = detect_program_hit(&tx, &cfg).unwrap();
        assert!(hit.program_hit);
        assert!(!hit.authority_hit);
    }

    #[test]
    fn detect_hits_on_authority() {
        let authority = Pubkey::new_from_array([6u8; 32]);
        let other_prog = Pubkey::new_from_array([5u8; 32]);
        let cfg = watch_cfg_with(vec![other_prog], vec![authority], Arc::new(NoopMintFinder));
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, other_prog, authority];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        let hit = detect_program_hit(&tx, &cfg).unwrap();
        assert!(hit.authority_hit);
        assert!(hit.program_hit);
    }

    #[test]
    fn detect_both_program_and_authority() {
        let prog = Pubkey::new_from_array([5u8; 32]);
        let authority = Pubkey::new_from_array([6u8; 32]);
        let cfg = watch_cfg_with(vec![prog], vec![authority], Arc::new(NoopMintFinder));
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, prog, authority];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        let hit = detect_program_hit(&tx, &cfg).unwrap();
        assert!(hit.program_hit);
        assert!(hit.authority_hit);
    }

    #[test]
    fn detect_filters_by_fee_payer() {
        let prog = Pubkey::new_from_array([5u8; 32]);
        let allowed_payer = Pubkey::new_from_array([1u8; 32]);
        let wrong_payer = Pubkey::new_from_array([2u8; 32]);
        let mut cfg = watch_cfg_with(vec![prog], vec![], Arc::new(NoopMintFinder));
        cfg.fee_payers = std::collections::HashSet::from([allowed_payer]);

        let keys = vec![wrong_payer, prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        assert!(detect_program_hit(&tx, &cfg).is_none());
    }

    #[test]
    fn detect_skips_vote_tx_when_configured() {
        let vote_prog = vote_program_id();
        let mut cfg = watch_cfg_with(vec![vote_prog], vec![], Arc::new(NoopMintFinder));
        cfg.skip_vote_txs = true;
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, vote_prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        assert!(detect_program_hit(&tx, &cfg).is_none());
    }

    #[test]
    fn detect_passes_vote_tx_when_not_skipped() {
        let vote_prog = vote_program_id();
        let mut cfg = watch_cfg_with(vec![vote_prog], vec![], Arc::new(NoopMintFinder));
        cfg.skip_vote_txs = false;
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, vote_prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        assert!(detect_program_hit(&tx, &cfg).is_some());
    }

    #[test]
    fn detect_populates_mints_from_finder() {
        let prog = Pubkey::new_from_array([5u8; 32]);
        let mint = Pubkey::new_from_array([0xaau8; 32]);
        let fixed_finder = Arc::new(FixedMintFinder {
            mints: vec![MintInfo {
                mint,
                label: Some("test"),
            }],
        });
        let cfg = watch_cfg_with(vec![prog], vec![], fixed_finder);
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx = make_tx(keys, vec![ix]);
        let hit = detect_program_hit(&tx, &cfg).unwrap();
        assert_eq!(hit.mints.len(), 1);
        assert_eq!(hit.mints[0].mint, mint);
    }

    #[test]
    fn first_signatures_respects_limit() {
        let prog = Pubkey::new_from_array([5u8; 32]);
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let tx1 = make_tx(keys.clone(), vec![ix.clone()]);
        let tx2 = make_tx(keys, vec![ix]);
        let sigs = first_signatures([&tx1, &tx2], 1, false);
        assert_eq!(sigs.len(), 1);
    }

    #[test]
    fn first_signatures_skips_vote_txs() {
        let vote_prog = vote_program_id();
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, vote_prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let vote_tx = make_tx(keys, vec![ix]);
        let sigs = first_signatures([&vote_tx], 10, true);
        assert!(sigs.is_empty());
    }

    #[test]
    fn first_signatures_includes_vote_txs_when_allowed() {
        let vote_prog = vote_program_id();
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let keys = vec![fee_payer, vote_prog];
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        };
        let vote_tx = make_tx(keys, vec![ix]);
        let sigs = first_signatures([&vote_tx], 10, false);
        assert_eq!(sigs.len(), 1);
    }

    fn default_spl_cfg() -> ProgramWatchConfig {
        ProgramWatchConfig::new(vec![], vec![])
    }

    #[test]
    fn spl_finder_detects_initialize_mint() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let mint = Pubkey::new_from_array([0xbbu8; 32]);
        let keys = vec![fee_payer, mint, TOKEN_PROGRAM];
        let ix = CompiledInstruction {
            program_id_index: 2,
            accounts: vec![1],
            data: vec![0],
        };
        let tx = make_tx(keys, vec![ix]);
        let cfg = default_spl_cfg();
        let finder = SplTokenMintFinder;
        let mints = finder.find_mints(&tx, &cfg);
        assert_eq!(mints.len(), 1);
        assert_eq!(mints[0].mint, mint);
    }

    #[test]
    fn spl_finder_detects_mint_to() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let mint = Pubkey::new_from_array([0xccu8; 32]);
        let keys = vec![fee_payer, mint, TOKEN_PROGRAM];
        let ix = CompiledInstruction {
            program_id_index: 2,
            accounts: vec![1],
            data: vec![7],
        };
        let tx = make_tx(keys, vec![ix]);
        let cfg = default_spl_cfg();
        let finder = SplTokenMintFinder;
        let mints = finder.find_mints(&tx, &cfg);
        assert_eq!(mints.len(), 1);
        assert_eq!(mints[0].mint, mint);
    }

    #[test]
    fn spl_finder_ignores_non_token_program() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let mint = Pubkey::new_from_array([0xccu8; 32]);
        let other_prog = Pubkey::new_from_array([9u8; 32]);
        let keys = vec![fee_payer, mint, other_prog];
        let ix = CompiledInstruction {
            program_id_index: 2,
            accounts: vec![1],
            data: vec![0],
        };
        let tx = make_tx(keys, vec![ix]);
        let cfg = default_spl_cfg();
        let finder = SplTokenMintFinder;
        let mints = finder.find_mints(&tx, &cfg);
        assert!(mints.is_empty());
    }

    fn pumpfun_program() -> Pubkey {
        Pubkey::new_from_array([0xeeu8; 32])
    }

    fn pumpfun_finder() -> PumpfunAccountMintFinder {
        PumpfunAccountMintFinder::new(vec![pumpfun_program()])
    }

    fn cfg_noop() -> ProgramWatchConfig {
        ProgramWatchConfig::new(vec![], vec![])
    }

    #[test]
    fn pumpfun_finder_create_v2() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let mint = Pubkey::new_from_array([0xaau8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, mint];
        let mut data = PUMPFUN_CREATE_V2_DISC.to_vec();
        data.extend_from_slice(&[0u8; 8]);
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![2, 0],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let mints = pumpfun_finder().find_mints(&tx, &cfg_noop());
        assert_eq!(mints.len(), 1);
        assert_eq!(mints[0].mint, mint);
    }

    #[test]
    fn pumpfun_finder_buy() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let acc1 = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([0xaau8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, acc1, mint];
        let mut data = PUMPFUN_BUY_DISC.to_vec();
        data.extend_from_slice(&[0u8; 16]);
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0, 2, 3],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let mints = pumpfun_finder().find_mints(&tx, &cfg_noop());
        assert_eq!(mints.len(), 1);
        assert_eq!(mints[0].mint, mint);
    }

    #[test]
    fn pumpfun_finder_sell() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let acc1 = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([0xbbu8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, acc1, mint];
        let mut data = PUMPFUN_SELL_DISC.to_vec();
        data.extend_from_slice(&[0u8; 16]);
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0, 2, 3],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let mints = pumpfun_finder().find_mints(&tx, &cfg_noop());
        assert_eq!(mints.len(), 1);
        assert_eq!(mints[0].mint, mint);
    }

    #[test]
    fn pumpfun_finder_skips_system_id() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let system = SYSTEM_PROGRAM_ID;
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, system];
        let mut data = PUMPFUN_CREATE_V2_DISC.to_vec();
        data.extend_from_slice(&[0u8; 8]);
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![2],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let mints = pumpfun_finder().find_mints(&tx, &cfg_noop());
        assert!(mints.is_empty());
    }

    fn build_create_v2_data(
        name: &str,
        symbol: &str,
        uri: &str,
        creator: &Pubkey,
        is_mayhem: bool,
        is_cashback: bool,
    ) -> Vec<u8> {
        let mut data = PUMPFUN_CREATE_V2_DISC.to_vec();
        for s in [name, symbol, uri] {
            let bytes = s.as_bytes();
            data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(bytes);
        }
        data.extend_from_slice(creator.as_ref());
        data.push(u8::from(is_mayhem));
        data.push(u8::from(is_cashback));
        data
    }

    #[test]
    fn parse_creator_valid() {
        let creator = Pubkey::new_from_array([0xddu8; 32]);
        let data = build_create_v2_data(
            "TestToken",
            "TT",
            "https://example.com",
            &creator,
            true,
            false,
        );
        let result = parse_create_v2_creator(&data);
        assert!(result.is_some());
        let (parsed_creator, is_mayhem, is_cashback) = result.unwrap();
        assert_eq!(parsed_creator, creator);
        assert!(is_mayhem);
        assert!(!is_cashback);
    }

    #[test]
    fn parse_creator_truncated() {
        let data = PUMPFUN_CREATE_V2_DISC.to_vec();
        assert!(parse_create_v2_creator(&data).is_none());
    }

    fn pump_detailer() -> PumpfunDetailer {
        PumpfunDetailer::new(vec![pumpfun_program()])
    }

    #[test]
    fn detailer_create_sets_action() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let mint = Pubkey::new_from_array([0xaau8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, mint];
        let mut data = PUMPFUN_CREATE_DISC.to_vec();
        data.extend_from_slice(&[0u8; 16]);
        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![2],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let cfg = cfg_noop();
        let mints = vec![MintInfo {
            mint,
            label: Some("pump:create"),
        }];
        let details = pump_detailer().detail(&tx, &cfg, &mints);
        assert!(!details.is_empty());
        let d = details.iter().find(|d| d.mint == mint).unwrap();
        assert_eq!(d.action, Some("create"));
    }

    #[test]
    fn detailer_buy_extracts_amounts() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let acc1 = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([0xaau8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, acc1, mint];

        let token_amount: u64 = 1_000;
        let sol_amount: u64 = 500_000_000;
        let mut data = PUMPFUN_BUY_DISC.to_vec();
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&sol_amount.to_le_bytes());

        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0, 2, 3],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let cfg = cfg_noop();
        let mints = vec![MintInfo {
            mint,
            label: Some("pump:buy"),
        }];
        let details = pump_detailer().detail(&tx, &cfg, &mints);
        let d = details.iter().find(|d| d.mint == mint).unwrap();
        assert_eq!(d.action, Some("buy"));
        assert_eq!(d.token_amount, Some(token_amount));
        assert_eq!(d.sol_amount, Some(sol_amount));
    }

    #[test]
    fn detailer_buy_exact_extracts_sol_only() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let acc1 = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([0xaau8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump, acc1, mint];

        let sol_amount: u64 = 200_000_000;
        let mut data = PUMPFUN_BUY_EXACT_SOL_IN_DISC.to_vec();
        data.extend_from_slice(&sol_amount.to_le_bytes());

        let ix = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0, 2, 3],
            data,
        };
        let tx = make_tx(keys, vec![ix]);
        let cfg = cfg_noop();
        let mints = vec![MintInfo {
            mint,
            label: Some("pump:buy_exact"),
        }];
        let details = pump_detailer().detail(&tx, &cfg, &mints);
        let d = details.iter().find(|d| d.mint == mint).unwrap();
        assert_eq!(d.action, Some("buy"));
        assert_eq!(d.sol_amount, Some(sol_amount));
        assert!(d.token_amount.is_none());
    }

    #[test]
    fn detailer_backfills_from_mint_info() {
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let pump = pumpfun_program();
        let keys = vec![fee_payer, pump];
        let tx = make_tx(keys, vec![]);
        let cfg = cfg_noop();
        let mint = Pubkey::new_from_array([0xffu8; 32]);
        let mints = vec![MintInfo {
            mint,
            label: Some("pump:buy"),
        }];
        let details = pump_detailer().detail(&tx, &cfg, &mints);
        let d = details.iter().find(|d| d.mint == mint).unwrap();
        assert_eq!(d.action, Some("buy"));
    }

    #[test]
    fn composite_merges_and_deduplicates() {
        let mint_a = Pubkey::new_from_array([0xaau8; 32]);
        let mint_b = Pubkey::new_from_array([0xbbu8; 32]);
        let finder_a = Arc::new(FixedMintFinder {
            mints: vec![
                MintInfo {
                    mint: mint_a,
                    label: Some("a"),
                },
                MintInfo {
                    mint: mint_b,
                    label: None,
                },
            ],
        });
        let finder_b = Arc::new(FixedMintFinder {
            mints: vec![MintInfo {
                mint: mint_b,
                label: Some("b"),
            }],
        });
        let composite = CompositeMintFinder::new(vec![finder_a, finder_b]);
        let fee_payer = Pubkey::new_from_array([0u8; 32]);
        let tx = make_tx(vec![fee_payer], vec![]);
        let cfg = cfg_noop();
        let mints = composite.find_mints(&tx, &cfg);

        let a_count = mints.iter().filter(|m| m.mint == mint_a).count();
        let b_count = mints.iter().filter(|m| m.mint == mint_b).count();
        assert_eq!(a_count, 1);
        assert_eq!(b_count, 1);
        let mb = mints.iter().find(|m| m.mint == mint_b).unwrap();
        assert_eq!(mb.label, Some("b"));
    }
}
