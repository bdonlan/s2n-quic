// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(unused)]

use super::*;
use bolero::check;
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
    opened_local_bidi_streams: u64,
    closed_local_bidi_streams: u64,

    opened_remote_bidi_streams: u64,
    closed_remote_bidi_streams: u64,

    opened_local_uni_streams: u64,
    closed_local_uni_streams: u64,

    opened_remote_uni_streams: u64,
    closed_remote_uni_streams: u64,
}

impl Oracle {
    fn on_open_stream(&mut self, _id: u8, stream_direction: StreamDirection) {
        // TODO open more than 1 stream potentially
        match stream_direction {
            StreamDirection::LocalInitiatedBidirectional => self.opened_local_bidi_streams += 1,
            StreamDirection::RemoteInitiatedBidirectional => self.opened_remote_bidi_streams += 1,
            StreamDirection::LocalInitiatedUnidirectional => self.opened_local_uni_streams += 1,
            StreamDirection::RemoteInitiatedUnidirectional => self.opened_remote_uni_streams += 1,
        }
    }

    fn on_close_stream(&mut self, _id: u8, stream_direction: StreamDirection) {
        // TODO confirm stream has been opened
        match stream_direction {
            StreamDirection::LocalInitiatedBidirectional => self.closed_local_bidi_streams += 1,
            StreamDirection::RemoteInitiatedBidirectional => self.closed_remote_bidi_streams += 1,
            StreamDirection::LocalInitiatedUnidirectional => self.closed_local_uni_streams += 1,
            StreamDirection::RemoteInitiatedUnidirectional => self.closed_remote_uni_streams += 1,
        }
    }
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
        match operation {
            Operation::OpenStream {
                id,
                stream_direction,
            } => self.on_open_stream(*id, *stream_direction),
            Operation::CloseStream {
                id,
                stream_direction,
            } => self.on_close_stream(*id, *stream_direction),
        }
    }

    /// Check that the subject and oracle match.
    pub fn invariants(&self) {
        assert!(self.oracle.opened_local_uni_streams >= self.oracle.closed_local_uni_streams);
        assert!(self.oracle.opened_local_bidi_streams >= self.oracle.closed_local_bidi_streams);
        assert!(self.oracle.opened_remote_uni_streams >= self.oracle.closed_remote_uni_streams);
        assert!(self.oracle.opened_local_bidi_streams >= self.oracle.closed_local_bidi_streams);
    }

    fn on_open_stream(&mut self, id: u8, stream_direction: StreamDirection) {
        self.oracle.on_open_stream(id, stream_direction);
        // self.subject.on_open_stream()
    }

    fn on_close_stream(&mut self, id: u8, stream_direction: StreamDirection) {
        self.oracle.on_close_stream(id, stream_direction);
        // self.subject.on_open_stream()
    }
}

#[derive(Debug, TypeGenerator)]
enum Operation {
    OpenStream {
        #[generator(0..5)]
        id: u8,
        stream_direction: StreamDirection,
    },
    CloseStream {
        #[generator(0..5)]
        id: u8,
        stream_direction: StreamDirection,
    },
}

// #[test]
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
