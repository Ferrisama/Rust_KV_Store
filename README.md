# Rust Distributed Key-Value Store

This project is a distributed key-value store implemented in Rust. It features a cluster of nodes that work together to store and retrieve data, with support for replication to ensure fault tolerance.

## Features

- Distributed architecture with multiple nodes
- Consistent hashing for data distribution
- Basic CRUD operations (Create, Read, Update, Delete)
- Primary-backup replication for fault tolerance
- Simple client with support for node redirects

## Getting Started

### Prerequisites

- Rust (latest stable version)
- Cargo (comes with Rust)

### Running the project

1. Clone the repository:

   ```
   git clone https://github.com/Ferrisama/Rust_KV_Store.git
   cd Rust_KV_Store
   ```

2. Build and run the project:
   ```
   RUST_LOG=info cargo run
   ```

## Project Structure

- `src/main.rs`: Entry point of the application
- `src/node/mod.rs`: Implementation of individual nodes
- `src/client/mod.rs`: Client implementation for interacting with the cluster
- `src/cluster/mod.rs`: Cluster management and consistent hashing implementation

## Future Improvements

- Node failure detection and automatic data redistribution
- Support for tunable consistency levels
- Range queries and support for complex data types
- Monitoring and metrics collection
- Security enhancements (authentication, authorization, encryption)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
