// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bolero::{check, generator::*};
use s2n_quic_core::transmission::Mode;

#[derive(Debug)]
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
    fn new() -> Self {
        // Model {
        //     oracle: Oracle::default(),
        //     subject: Controller::new(
        //         local_endpoint_type,
        //         initial_peer_limits,
        //         initial_local_limits,
        //         stream_limits,
        //     ),
        // }

        todo!()
    }

    pub fn apply(&mut self, operation: &Operation) {
        todo!()
    }

    /// Check that the subject and oracle match.
    pub fn invariants(&self) {
        todo!()
    }
}

#[derive(Debug, TypeGenerator)]
enum Operation {
    OpenStream,
    CloseStream,
}

#[test]
fn model_test() {
    check!()
        .with_type::<Vec<Operation>>()
        .for_each(|operations| {
            let mut model = Model::new();
            for operation in operations.iter() {
                model.apply(operation);
            }

            model.invariants();
        })
}
