//! Resolver that tries multiple DNS servers and returns the first successful answer.

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use crate::address::NetLocation;
use crate::resolver::Resolver;

/// Resolver that queries multiple DNS servers concurrently until one succeeds.
pub struct CompositeResolver {
    resolvers: Vec<Arc<dyn Resolver>>,
}

impl Debug for CompositeResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeResolver")
            .field("count", &self.resolvers.len())
            .finish()
    }
}

impl CompositeResolver {
    pub fn new(resolvers: Vec<Arc<dyn Resolver>>) -> Self {
        Self { resolvers }
    }
}

impl Resolver for CompositeResolver {
    fn resolve_location(
        &self,
        location: &NetLocation,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send>> {
        let resolvers = self.resolvers.clone();
        let location = location.clone();

        Box::pin(async move {
            if resolvers.is_empty() {
                return Err(std::io::Error::other("no DNS resolvers configured"));
            }

            let mut pending = FuturesUnordered::new();
            for (i, resolver) in resolvers.into_iter().enumerate() {
                let location = location.clone();
                pending.push(async move {
                    let resolver_debug = format!("{resolver:?}");
                    let result = resolver.resolve_location(&location).await;
                    (i, resolver_debug, result)
                });
            }

            let mut errors = Vec::new();
            while let Some((i, resolver_debug, result)) = pending.next().await {
                match result {
                    Ok(addrs) if !addrs.is_empty() => {
                        if i > 0 {
                            log::info!(
                                "CompositeResolver: resolved {} via resolver #{} ({})",
                                location,
                                i,
                                resolver_debug
                            );
                        }
                        return Ok(addrs);
                    }
                    Ok(_) => {
                        log::debug!(
                            "CompositeResolver: resolver #{} ({}) returned empty for {}",
                            i,
                            resolver_debug,
                            location
                        );
                        errors.push(format!("resolver #{i} returned empty response"));
                    }
                    Err(e) => {
                        log::debug!(
                            "CompositeResolver: resolver #{} ({}) failed for {}: {}",
                            i,
                            resolver_debug,
                            location,
                            e
                        );
                        errors.push(format!("resolver #{i} failed: {e}"));
                    }
                }
            }

            Err(std::io::Error::other(format!(
                "all DNS resolvers failed for {}: {}",
                location,
                errors.join("; ")
            )))
        })
    }
}
