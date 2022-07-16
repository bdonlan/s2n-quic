// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(unused)]

use super::*;
use bolero::{check, generator::*};
use futures_test::task::new_count_waker;
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
    fn on_open_stream(&mut self, _id: u8) {
        // TODO open more than 1 stream potentially
    }

    fn on_close_stream(&mut self, _id: u8) {
        // TODO confirm stream has been opened
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
            Operation::OpenStream { id } => self.on_open_stream(*id),
            Operation::CloseStream { id } => self.on_close_stream(*id),
        }
    }

    /// Check that the subject and oracle match.
    pub fn invariants(&self) {
        assert!(self.oracle.opened_local_uni_streams >= self.oracle.closed_local_uni_streams);
        assert!(self.oracle.opened_local_bidi_streams >= self.oracle.closed_local_bidi_streams);
        assert!(self.oracle.opened_remote_uni_streams >= self.oracle.closed_remote_uni_streams);
        assert!(self.oracle.opened_local_bidi_streams >= self.oracle.closed_local_bidi_streams);
    }

    fn on_open_stream(&mut self, id: u8) {
        let stream_id = StreamId::from_varint(VarInt::from_u32(id as u32));
        self.oracle.on_open_stream(id);
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        match self.subject.direction(stream_id) {
            StreamDirection::LocalInitiatedBidirectional
            | StreamDirection::LocalInitiatedUnidirectional => {
                self.subject
                    .poll_open_local_stream(stream_id, &mut token, &Context::from_waker(&waker))
                    .is_ready();
            }
            StreamDirection::RemoteInitiatedBidirectional
            | StreamDirection::RemoteInitiatedUnidirectional => {
                let stream_iter = StreamIter::new(stream_id, stream_id);
                self.subject.on_open_remote_stream(stream_iter).unwrap()
            }
        }
    }

    fn on_close_stream(&mut self, id: u8) {
        let stream_id = StreamId::from_varint(VarInt::from_u32(id as u32));
        self.oracle.on_close_stream(id);
        self.subject.on_close_stream(stream_id);
    }
}

#[derive(Debug, TypeGenerator)]
enum Operation {
    OpenStream {
        #[generator(0..200)]
        id: u8,
    },
    CloseStream {
        #[generator(0..5)]
        id: u8,
    },
}

#[test]
fn model_test() {
    check!()
        .with_type::<Vec<Operation>>()
        .for_each(|operations| {
            let local_endpoint_type = endpoint::Type::Server;

            let mut model = Model::new(local_endpoint_type);
            for operation in operations.iter() {
                model.apply(operation);
            }

            model.invariants();
        })
}
