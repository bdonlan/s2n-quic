// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(unused)]

use super::*;
use bolero::{check, generator::*};
use futures_test::task::new_count_waker;
use s2n_quic_core::{transport::parameters::InitialStreamLimits, varint::VarInt};

#[derive(Debug)]
struct Oracle {
    local_endpoint_type: endpoint::Type,
    stream_limits: stream::Limits,
    initial_local_limits: InitialFlowControlLimits,
    initial_remote_limits: InitialFlowControlLimits,

    max_remote_bidi_opened_id: Option<u64>,

    max_remote_uni_opened_id: Option<u64>,
    // opened_local_bidi_streams: u64,
    // closed_local_bidi_streams: u64,

    // opened_remote_bidi_streams: u64,
    // closed_remote_bidi_streams: u64,

    // opened_local_uni_streams: u64,
    // closed_local_uni_streams: u64,

    // opened_remote_uni_streams: u64,
    // closed_remote_uni_streams: u64,
}

impl Oracle {}

#[derive(Debug)]
struct Model {
    oracle: Oracle,
    subject: Controller,
}

impl Model {
    fn new(local_endpoint_type: endpoint::Type, limit: u32) -> Self {
        let mut initial_local_limits = InitialFlowControlLimits::default();
        let initial_remote_limits = InitialFlowControlLimits::default();
        let stream_limits = stream::Limits::default();

        initial_local_limits.max_streams_bidi = VarInt::from_u32(limit);
        initial_local_limits.max_streams_uni = VarInt::from_u32(limit);

        Model {
            oracle: Oracle {
                local_endpoint_type,
                stream_limits,
                initial_local_limits,
                initial_remote_limits,
                max_remote_bidi_opened_id: None,
                max_remote_uni_opened_id: None,
            },
            subject: Controller::new(
                local_endpoint_type,
                initial_remote_limits,
                initial_local_limits,
                stream_limits,
            ),
        }
    }

    pub fn apply(&mut self, operation: &Operation) {
        match operation {
            Operation::OpenRemoteBidi { nth_id } => self.on_open_remote_bidi(*nth_id as u64),
            Operation::OpenRemoteUni { nth_id } => self.on_open_remote_uni(*nth_id as u64),
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

    fn on_open_remote_bidi(&mut self, nth_id: u64) {
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        let stream_initiator = self.oracle.local_endpoint_type.peer_type();
        let stream_type = StreamType::Bidirectional;

        let nth_cnt = nth_id + 1;
        let stream_id = StreamId::nth(
            self.oracle.local_endpoint_type.peer_type(),
            stream_type,
            nth_id,
        )
        .unwrap();

        // self.oracle.on_remote_bidi(stream_id);

        let stream_iter =
            if let Some(max_remote_bidi_opened_id) = self.oracle.max_remote_bidi_opened_id {
                // id already opened.. return
                if max_remote_bidi_opened_id >= nth_id {
                    return;
                }
                let max_opened_stream_id = StreamId::nth(
                    self.oracle.local_endpoint_type.peer_type(),
                    stream_type,
                    max_remote_bidi_opened_id,
                )
                .unwrap();

                // next id to open
                StreamIter::new(max_opened_stream_id.next_of_type().unwrap(), stream_id)
            } else {
                let initial =
                    StreamId::initial(self.oracle.local_endpoint_type.peer_type(), stream_type);
                StreamIter::new(initial, stream_id)
            };
        self.oracle.max_remote_bidi_opened_id = Some(nth_id);

        let res = self.subject.on_open_remote_stream(stream_iter);

        if nth_cnt > self.oracle.initial_local_limits.max_streams_bidi.as_u64() {
            res.expect_err("limts violated");
        } else {
            res.unwrap();
        }
    }

    fn on_open_remote_uni(&mut self, nth_id: u64) {
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        let stream_initiator = self.oracle.local_endpoint_type.peer_type();
        let stream_type = StreamType::Unidirectional;

        let nth_cnt = nth_id + 1;
        let stream_id = StreamId::nth(
            self.oracle.local_endpoint_type.peer_type(),
            stream_type,
            nth_id,
        )
        .unwrap();

        // self.oracle.on_remote_uni(stream_id);

        let stream_iter =
            if let Some(max_remote_uni_opened_id) = self.oracle.max_remote_uni_opened_id {
                // id already opened.. return
                if max_remote_uni_opened_id >= nth_id {
                    return;
                }
                let max_opened_stream_id = StreamId::nth(
                    self.oracle.local_endpoint_type.peer_type(),
                    stream_type,
                    max_remote_uni_opened_id,
                )
                .unwrap();

                // next id to open
                StreamIter::new(max_opened_stream_id.next_of_type().unwrap(), stream_id)
            } else {
                let initial =
                    StreamId::initial(self.oracle.local_endpoint_type.peer_type(), stream_type);
                StreamIter::new(initial, stream_id)
            };
        self.oracle.max_remote_uni_opened_id = Some(nth_id);

        let res = self.subject.on_open_remote_stream(stream_iter);

        if nth_cnt > self.oracle.initial_local_limits.max_streams_uni.as_u64() {
            res.expect_err("limts violated");
        } else {
            res.unwrap();
        }
    }

    fn on_close_stream(&mut self, nth_id: u16) {
        let stream_id = StreamId::from_varint(VarInt::from_u32(nth_id as u32));
        // self.oracle.on_close_stream(nth_id);
        self.subject.on_close_stream(stream_id);
    }
}

#[derive(Debug, TypeGenerator)]
enum Operation {
    // max_local_limit: max_remote_uni_stream (declared locally)
    // tranmit: max_streams
    OpenRemoteBidi {
        #[generator(0..200)]
        nth_id: u16,
    },
    // CloseRemoteBidi {
    //     #[generator(0..100)]
    //     id: u16,
    // },

    // max_local_limit: max_remote_uni_stream (declared locally)
    // tranmit: max_streams
    OpenRemoteUni {
        #[generator(0..200)]
        nth_id: u16,
    },
}

// LocalBidirectional
// max_local_limit: max_local_bidi_stream
// peer_stream_limit: peer_max_bidi_stream
//
// limits: max_local_bidi_stream.min(peer_max_bidi_stream)
// tranmit: streams_blocked

// LocalUnidirectional
// max_local_limit: max_local_uni_stream
// peer_stream_limit: peer_max_uni_stream
//
// limits: max_local_uni_stream.min(peer_max_uni_stream)
// tranmit: streams_blocked

// RemoteBidirectional
// max_local_limit: max_remote_bidi_stream (declared locally)
// tranmit: max_streams

// RemoteUnidirectional
// max_local_limit: max_remote_uni_stream (declared locally)
// tranmit: max_streams
#[test]
fn model_test() {
    check!()
        .with_type::<(u16, Vec<Operation>)>()
        .for_each(|(limit, operations)| {
            // let bidi_stream = StreamId::from_u32(512); // client bidi
            // let bidi_stream = StreamId::from_u32(4); // client bidi
            // let bidi_stream = StreamId::from_u32(256); // client bidi
            // let bidi_stream = StreamId::from_u32(0); // client bidi
            // let bidi_stream = StreamId::from_u32(11); // server uni

            let local_endpoint_type = endpoint::Type::Server;

            let mut model = Model::new(local_endpoint_type, *limit as u32);
            for operation in operations.iter() {
                model.apply(operation);
            }

            model.invariants();
        })
}
