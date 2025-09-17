# nim-raft
# Copyright (c) 2023 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

# Enable Nim's profiler when compiled with --profiler:on
when compileOption("profiler"):
  import nimprof

import test_consensus_state_machine
import test_log
import test_state
import test_bls_cluester
import test_conflict_optimization
import test_leader_stepdown
export test_log
export test_state
export test_bls_cluester
export test_consensus_state_machine
export test_conflict_optimization
export test_leader_stepdown
