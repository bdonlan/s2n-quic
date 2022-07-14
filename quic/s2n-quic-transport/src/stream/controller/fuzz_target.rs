// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bolero::{check, generator::*};
use s2n_quic_core::{transport::parameters::InitialStreamLimits, varint::VarInt};

fn create_default_initial_flow_control_limits() -> InitialFlowControlLimits {
    InitialFlowControlLimits {
        stream_limits: InitialStreamLimits {
            max_data_bidi_local: VarInt::from_u32(4096),
            max_data_bidi_remote: VarInt::from_u32(4096),
            max_data_uni: VarInt::from_u32(4096),
        },
        max_data: VarInt::from_u32(64 * 1024),
        max_streams_bidi: VarInt::from_u32(128),
        max_streams_uni: VarInt::from_u32(128),
    }
}

#[derive(Debug, Default)]
struct Oracle {
    local_opened_streams: u64,
    remote_opened_streams: u64,
    local_closed_streams: u64,
    remote_closed_streams: u64,
}

#[derive(Debug)]
struct Model {
    oracle: Oracle,
    subject: Controller,
}

impl Model {
    fn new(local_endpoint_type: endpoint::Type) -> Self {
        let initial_local_limits = create_default_initial_flow_control_limits();
        let initial_peer_limits = create_default_initial_flow_control_limits();

        let stream_limits = stream::limits::Limits::default();

        Model {
            oracle: Oracle::default(),
            subject: Controller::new(
                local_endpoint_type,
                initial_peer_limits,
                initial_local_limits,
                stream_limits,
            ),
        }
    }

    pub fn apply(&mut self, operation: &Operation) {
        todo!()
    }

    /// Check that the subject and oracle match.
    pub fn invariants(&self) {
        assert!(true);
    }
}

#[derive(Debug, TypeGenerator)]
enum Operation {
    OpenStream,
    CloseStream,
}

#[test]
fn model_fuzz() {
    check!()
        .with_type::<Vec<Operation>>()
        .for_each(|operations| {
            // TODO get these from fuzzing
            let local_endpoint_type = endpoint::Type::Server;

            let mut model = Model::new(local_endpoint_type);
            for operation in operations.iter() {
                model.apply(operation);
            }

            model.invariants();
        })
}
