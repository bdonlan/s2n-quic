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

#[derive(Debug)]
struct Oracle {
    local_endpoint_type: endpoint::Type,
    uni_limit: u64,
    bidi_limit: u64,
    // opened_local_bidi_streams: u64,
    // closed_local_bidi_streams: u64,

    // opened_remote_bidi_streams: u64,
    // closed_remote_bidi_streams: u64,

    // opened_local_uni_streams: u64,
    // closed_local_uni_streams: u64,

    // opened_remote_uni_streams: u64,
    // closed_remote_uni_streams: u64,
}

impl Oracle {
    fn on_open_stream(&mut self, _id: u16) {
        // TODO open more than 1 stream potentially
    }

    fn on_close_stream(&mut self, _id: u16) {
        // TODO confirm stream has been opened
    }
}

#[derive(Debug)]
struct Model {
    oracle: Oracle,
    subject: Controller,
}

impl Model {
    fn new(local_endpoint_type: endpoint::Type, uni_limit: u64, bidi_limit: u64) -> Self {
        let initial_local_limits = create_default_initial_flow_control_limits();
        let initial_peer_limits = create_default_initial_flow_control_limits();

        let stream_limits = stream::Limits {
            max_send_buffer_size: 4096.try_into().unwrap(),
            max_open_local_unidirectional_streams: uni_limit.try_into().unwrap(),
            max_open_local_bidirectional_streams: bidi_limit.try_into().unwrap(),
        };

        Model {
            oracle: Oracle {
                local_endpoint_type,
                uni_limit,
                bidi_limit,
            },
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
            // Operation::CloseStream { id } => self.on_close_stream(*id),
        }
    }

    /// Check that the subject and oracle match.
    pub fn invariants(&self) {
        // assert!(self.oracle.opened_local_uni_streams >= self.oracle.closed_local_uni_streams);
        // assert!(self.oracle.opened_local_bidi_streams >= self.oracle.closed_local_bidi_streams);
        // assert!(self.oracle.opened_remote_uni_streams >= self.oracle.closed_remote_uni_streams);
        // assert!(self.oracle.opened_local_bidi_streams >= self.oracle.closed_local_bidi_streams);
    }

    fn on_open_stream(&mut self, id: u16) {
        let stream_id = StreamId::from_varint(VarInt::from_u32(id as u32));
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        self.oracle.on_open_stream(id);

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
                let res = self.subject.on_open_remote_stream(stream_iter);
                let nth = stream_id.as_varint().as_u64() / 4;
                // TODO work on limits stuff
                if nth > self.oracle.bidi_limit {
                    res.expect_err("not err");
                } else {
                    res.unwrap();
                }
            }
        }
    }

    fn on_close_stream(&mut self, id: u16) {
        let stream_id = StreamId::from_varint(VarInt::from_u32(id as u32));
        self.oracle.on_close_stream(id);
        self.subject.on_close_stream(stream_id);
    }
}

#[derive(Debug, TypeGenerator)]
enum Operation {
    OpenStream {
        #[generator(0..1000)]
        id: u16,
    },
    // CloseStream {
    //     #[generator(0..5)]
    //     id: u16,
    // },
}

#[test]
fn model_test() {
    check!()
        .with_type::<(u16, u16, Vec<Operation>)>()
        .for_each(|(uni, bidi, operations)| {
            let local_endpoint_type = endpoint::Type::Server;

            let mut model = Model::new(local_endpoint_type, *uni as u64, *bidi as u64);
            for operation in operations.iter() {
                model.apply(operation);
            }

            model.invariants();
        })
}
