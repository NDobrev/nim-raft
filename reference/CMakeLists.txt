# Raft Consensus Algorithm - Reference Implementation (ScyllaDB)
# 
# This is a reference implementation from ScyllaDB used for comparison
# and understanding of the Raft protocol implementation patterns.
# The main nim-raft implementation is in ../src/raft/

add_library(raft STATIC)

target_sources(raft
  PRIVATE
    # Core Raft state machine and protocol logic
    fsm.cc          # Finite state machine (leader/follower/candidate transitions)
    raft.cc         # Main Raft interface and coordination
    
    # Log management and replication
    log.cc          # Raft log implementation with snapshot support
    
    # Server and networking
    server.cc       # Raft server facade and RPC handling
    
    # Progress tracking
    tracker.cc      # Follower progress tracking for replication
)

target_include_directories(raft
  PUBLIC
    ${CMAKE_SOURCE_DIR})

target_link_libraries(raft
  PUBLIC
    Seastar::seastar    # Async framework
    xxHash::xxhash)     # Fast hashing for checksums

check_headers(check-headers raft
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
