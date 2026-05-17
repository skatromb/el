use crate::{Destination, ElError, RunReport, Source};

/// Orchestrates a single end-to-end run from a `Source` to a `Destination`.
pub struct Transfer {
    source: Box<dyn Source>,
    destination: Box<dyn Destination>,
}

impl Transfer {
    /// Build a transfer.
    #[must_use]
    pub fn new(source: Box<dyn Source>, destination: Box<dyn Destination>) -> Self {
        Self { source, destination }
    }

    /// Resolve the schema, fetch partitions, hand them to the destination.
    ///
    /// # Errors
    /// Propagates any error from schema resolution, partition setup, or write.
    pub async fn run(self) -> Result<RunReport, ElError> {
        let schema = self.source.schema().await?;
        let partitions = self.source.partitions().await?;
        self.destination.write(schema, partitions).await
    }
}
