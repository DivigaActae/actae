// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

pub mod handler;
pub mod migrations;
pub mod page_token;
pub mod schema;
pub mod store;

pub use handler::EventHandler;
pub use page_token::{PageTokenClaims, PageTokenError, PAGE_TOKEN_TTL_SECS};
pub use schema::{
    AgentEvent, AgentEventType, ChannelMetadata, ClaimedWork, CreateGroupRequest,
    CreateWakeupRequest, EventMetadata, EventRecord, ForkInfo, ForkRequest, GroupInfo,
    GroupOffsetInfo, TransitionRequest, UpdateMetadataRequest, WakeupInfo,
};
pub use store::EventStore;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_type_size() {
        assert!(std::mem::size_of::<EventHandler>() > 0);
    }

    #[test]
    fn test_schema_types_size() {
        assert!(std::mem::size_of::<AgentEvent>() > 0);
        assert!(std::mem::size_of::<AgentEventType>() > 0);
        assert!(std::mem::size_of::<ForkInfo>() > 0);
        assert!(std::mem::size_of::<ChannelMetadata>() > 0);
        assert!(std::mem::size_of::<EventMetadata>() > 0);
        assert!(std::mem::size_of::<EventRecord>() > 0);
        assert!(std::mem::size_of::<ForkRequest>() > 0);
        assert!(std::mem::size_of::<UpdateMetadataRequest>() > 0);
    }

    #[test]
    fn test_store_type_size() {
        assert!(std::mem::size_of::<EventStore>() > 0);
    }
}
