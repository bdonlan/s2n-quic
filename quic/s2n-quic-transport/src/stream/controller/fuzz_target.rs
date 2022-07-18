// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![allow(unused)]

use super::*;
use bolero::{check, generator::*};
use futures_test::task::new_count_waker;
use s2n_quic_core::{
    stream::limits::LocalLimits, transport::parameters::InitialStreamLimits, varint::VarInt,
};

#[derive(Debug)]
struct Oracle {
    local_endpoint_type: endpoint::Type,
    stream_limits: stream::Limits,
    initial_local_limits: InitialFlowControlLimits,
    initial_remote_limits: InitialFlowControlLimits,

    max_remote_bidi_opened_id: Option<u64>,
    max_remote_uni_opened_id: Option<u64>,

    max_local_bidi_opened_nth_idx: Option<u64>,
    max_local_uni_opened_nth_idx: Option<u64>,
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
    fn new(local_endpoint_type: endpoint::Type, limits: Limits) -> Self {
        // let mut initial_local_limits = InitialFlowControlLimits::default();
        // let initial_remote_limits = InitialFlowControlLimits::default();
        // let stream_limits = stream::Limits::default();

        let (initial_local_limits, initial_remote_limits, stream_limits) =
            limits.as_contoller_limits();

        Model {
            oracle: Oracle {
                local_endpoint_type,
                stream_limits,
                initial_local_limits,
                initial_remote_limits,
                max_remote_bidi_opened_id: None,
                max_remote_uni_opened_id: None,
                max_local_bidi_opened_nth_idx: None,
                max_local_uni_opened_nth_idx: None,
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
            Operation::OpenLocalBidi { nth_id } => self.on_open_local_bidi(*nth_id as u64),
            Operation::OpenLocalUni { nth_id } => self.on_open_local_uni(*nth_id as u64),
        }
    }

    /// Check that the subject and oracle match.
    pub fn invariants(&self) {}

    fn on_open_local_bidi(&mut self, nth_id: u64) {
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        let stream_initiator = self.oracle.local_endpoint_type;
        let stream_type = StreamType::Bidirectional;

        let limit = self
            .oracle
            .initial_remote_limits
            .max_streams_bidi
            .min(
                self.oracle
                    .stream_limits
                    .max_open_local_bidirectional_streams
                    .as_varint(),
            )
            .as_u64();

        //-------------
        let stream_nth_idx_iter = if let Some(max_local_bidi_opened_nth_idx) =
            self.oracle.max_local_bidi_opened_nth_idx
        {
            // id already opened.. return
            if max_local_bidi_opened_nth_idx >= nth_id {
                return;
            }

            // +1 to get the next stream to open
            (max_local_bidi_opened_nth_idx + 1..=nth_id)
        } else {
            (0..=nth_id)
        };

        for stream_nth_idx in stream_nth_idx_iter {
            // the count is +1 since streams are 0-indexed
            let nth_cnt = stream_nth_idx + 1;
            let stream_id = StreamId::nth(stream_initiator, stream_type, stream_nth_idx).unwrap();

            let res = self.subject.poll_open_local_stream(
                stream_id,
                &mut token,
                &Context::from_waker(&waker),
            );

            // println!(
            //     "remlmi: {} applim: {} nth_id: {} iter_nth_id: {} nth_cnt: {} stream_id: {:?}",
            //     self.oracle.initial_remote_limits.max_streams_bidi,
            //     self.oracle
            //         .stream_limits
            //         .max_open_local_bidirectional_streams
            //         .as_varint(),
            //     nth_id,
            //     stream_nth_idx,
            //     nth_cnt,
            //     stream_id
            // );

            if nth_cnt > limit {
                assert!(res.is_pending())
            } else {
                assert!(res.is_ready());
                self.oracle.max_local_bidi_opened_nth_idx = Some(stream_nth_idx);
            }
        }
    }

    fn on_open_local_uni(&mut self, nth_id: u64) {
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        let stream_initiator = self.oracle.local_endpoint_type;
        let stream_type = StreamType::Unidirectional;

        let limit = self
            .oracle
            .initial_remote_limits
            .max_streams_uni
            .min(
                self.oracle
                    .stream_limits
                    .max_open_local_unidirectional_streams
                    .as_varint(),
            )
            .as_u64();

        //-------------
        let stream_nth_idx_iter =
            if let Some(max_local_uni_opened_nth_idx) = self.oracle.max_local_uni_opened_nth_idx {
                // id already opened.. return
                if max_local_uni_opened_nth_idx >= nth_id {
                    return;
                }

                // +1 to get the next stream to open
                (max_local_uni_opened_nth_idx + 1..=nth_id)
            } else {
                (0..=nth_id)
            };

        for stream_nth_idx in stream_nth_idx_iter {
            // the count is +1 since streams are 0-indexed
            let nth_cnt = stream_nth_idx + 1;
            let stream_id = StreamId::nth(stream_initiator, stream_type, stream_nth_idx).unwrap();

            let res = self.subject.poll_open_local_stream(
                stream_id,
                &mut token,
                &Context::from_waker(&waker),
            );

            // println!(
            //     "remlmi: {} applim: {} nth_id: {} iter_nth_id: {} nth_cnt: {} stream_id: {:?}",
            //     self.oracle.initial_remote_limits.max_streams_uni,
            //     self.oracle
            //         .stream_limits
            //         .max_open_local_unidirectional_streams
            //         .as_varint(),
            //     nth_id,
            //     stream_nth_idx,
            //     nth_cnt,
            //     stream_id
            // );

            if nth_cnt > limit {
                assert!(res.is_pending())
            } else {
                assert!(res.is_ready());
                self.oracle.max_local_uni_opened_nth_idx = Some(stream_nth_idx);
            }
        }
    }

    fn on_open_remote_bidi(&mut self, nth_id: u64) {
        let (waker, wake_counter) = new_count_waker();
        let mut token = connection::OpenToken::new();

        let stream_initiator = self.oracle.local_endpoint_type.peer_type();
        let stream_type = StreamType::Bidirectional;

        let nth_cnt = nth_id + 1;
        let stream_id = StreamId::nth(stream_initiator, stream_type, nth_id).unwrap();

        //-------------
        let stream_iter = if let Some(max_remote_bidi_opened_id) =
            self.oracle.max_remote_bidi_opened_id
        {
            // id already opened.. return
            if max_remote_bidi_opened_id >= nth_id {
                return;
            }
            let max_opened_stream_id =
                StreamId::nth(stream_initiator, stream_type, max_remote_bidi_opened_id).unwrap();

            // next id to open
            StreamIter::new(max_opened_stream_id.next_of_type().unwrap(), stream_id)
        } else {
            let initial = StreamId::initial(stream_initiator, stream_type);
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
        let stream_id = StreamId::nth(stream_initiator, stream_type, nth_id).unwrap();

        //-------------
        let stream_iter =
            if let Some(max_remote_uni_opened_id) = self.oracle.max_remote_uni_opened_id {
                // id already opened.. return
                if max_remote_uni_opened_id >= nth_id {
                    return;
                }
                let max_opened_stream_id =
                    StreamId::nth(stream_initiator, stream_type, max_remote_uni_opened_id).unwrap();

                // next id to open
                StreamIter::new(max_opened_stream_id.next_of_type().unwrap(), stream_id)
            } else {
                let initial = StreamId::initial(stream_initiator, stream_type);
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

    fn on_close_stream(&mut self, nth_id: u8) {
        let stream_id = StreamId::from_varint(VarInt::from_u32(nth_id as u32));
        // self.oracle.on_close_stream(nth_id);
        self.subject.on_close_stream(stream_id);
    }
}

#[test]
fn model_test() {
    check!()
        .with_type::<(Limits, Vec<Operation>)>()
        .for_each(|(limits, operations)| {
            // let bidi_stream = StreamId::from_u32(512); // client bidi
            // let bidi_stream = StreamId::from_u32(4); // client bidi
            // let bidi_stream = StreamId::from_u32(256); // client bidi
            // let bidi_stream = StreamId::from_u32(0); // client bidi
            // let bidi_stream = StreamId::from_u32(11); // server uni

            let local_endpoint_type = endpoint::Type::Server;

            let mut model = Model::new(local_endpoint_type, *limits);
            for operation in operations.iter() {
                model.apply(operation);
            }

            model.invariants();
        })
}

#[derive(Debug, TypeGenerator)]
#[allow(clippy::enum_variant_names)]
enum Operation {
    // max_local_limit: max_remote_uni_stream (initial_local_limits)
    // transmit: max_streams
    OpenRemoteBidi { nth_id: u8 },

    // max_local_limit: max_remote_uni_stream (initial_local_limits)
    // transmit: max_streams
    OpenRemoteUni { nth_id: u8 },

    // max_local_limit: max_local_bidi_stream
    // peer_stream_limit: peer_max_bidi_stream (initial_remote_limits)
    //
    // limits: max_local_bidi_stream.min(peer_max_bidi_stream)
    // transmit: streams_blocked
    OpenLocalBidi { nth_id: u8 },

    // max_local_limit: max_local_uni_stream
    // peer_stream_limit: peer_max_uni_stream (initial_remote_limits)
    //
    // limits: max_local_uni_stream.min(peer_max_uni_stream)
    // transmit: streams_blocked
    OpenLocalUni { nth_id: u8 },
}

#[derive(Debug, TypeGenerator, Clone, Copy)]
struct Limits {
    // OpenRemoteBidi (initial_local_limits)
    initial_local_max_remote_bidi: u8,

    // OpenRemoteUni (initial_local_limits)
    initial_local_max_remote_uni: u8,

    // OpenLocalBidi (initial_remote_limits)
    //  initial_remote_max_local_bidi.min(app_max_local_bidi)
    initial_remote_max_local_bidi: u8,
    app_max_local_bidi: u8,

    // OpenLocalUni (initial_remote_limits)
    //  initial_remote_max_local_uni.min(app_max_local_uni)
    initial_remote_max_local_uni: u8,
    app_max_local_uni: u8,
}

impl Limits {
    fn as_contoller_limits(
        &self,
    ) -> (
        InitialFlowControlLimits,
        InitialFlowControlLimits,
        stream::Limits,
    ) {
        let mut initial_local_limits = InitialFlowControlLimits::default();
        let mut initial_remote_limits = InitialFlowControlLimits::default();
        let stream_limits = stream::Limits {
            max_open_local_unidirectional_streams: (self.app_max_local_uni as u64)
                .try_into()
                .unwrap(),
            max_open_local_bidirectional_streams: (self.app_max_local_bidi as u64)
                .try_into()
                .unwrap(),
            ..Default::default()
        };

        // OpenRemoteBidi (initial_local_limits)
        initial_local_limits.max_streams_bidi =
            VarInt::from_u32(self.initial_local_max_remote_bidi.into());

        // OpenRemoteUni (initial_local_limits)
        initial_local_limits.max_streams_uni =
            VarInt::from_u32(self.initial_local_max_remote_uni.into());

        // OpenLocalBidi (initial_remote_limits)
        //  initial_remote_max_local_bidi.min(app_max_local_bidi)
        initial_remote_limits.max_streams_bidi =
            VarInt::from_u32(self.initial_remote_max_local_bidi.into());

        // OpenLocalUni (initial_remote_limits)
        //  initial_remote_max_local_uni.min(app_max_local_uni)
        initial_remote_limits.max_streams_uni =
            VarInt::from_u32(self.initial_remote_max_local_uni.into());

        (initial_local_limits, initial_remote_limits, stream_limits)
    }
}
